package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	chunker "github.com/ipfs/go-ipfs-chunker"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs/importer"
	"github.com/mitchellh/go-homedir"
	cli "github.com/urfave/cli/v2"
	"github.com/whyrusleeping/estuary/lib/retrievehelper"
	"golang.org/x/xerrors"
)

func main() {
	//--system dt-impl --system dt-chanmon --system dt_graphsync --system graphsync --system data_transfer_network debug
	logging.SetLogLevel("dt-impl", "debug")
	logging.SetLogLevel("dt-chanmon", "debug")
	logging.SetLogLevel("dt_graphsync", "debug")
	logging.SetLogLevel("data_transfer_network", "debug")
	app := cli.NewApp()

	app.Commands = []*cli.Command{
		makeDealCmd,
		getAskCmd,
		infoCmd,
		listDealsCmd,
		retrieveFileCmd,
		queryRetrievalCmd,
	}
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:  "repo",
			Value: "~/.lotus",
		},
	}

	// Store config dir in metadata
	ddir, err := homedir.Expand("~/.filc")
	if err != nil {
		fmt.Println("could not set config dir: ", err)
	}
	app.Metadata = map[string]interface{}{
		"ddir": ddir,
	}

	// ...and make sure the directory exists
	if err := os.MkdirAll(ddir, 0755); err != nil {
		fmt.Println("could not create config directory: ", err)
		os.Exit(1)
	}

	app.RunAndExitOnError()
}

// Get config directory from CLI metadata
func ddir(cctx *cli.Context) string {
	mDdir := cctx.App.Metadata["ddir"]
	switch ddir := mDdir.(type) {
	case string:
		return ddir
	default:
		panic("ddir should be present in CLI metadata")
	}
}

var makeDealCmd = &cli.Command{
	Name: "deal",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name: "miner",
		},
		&cli.BoolFlag{
			Name: "verified",
		},
	},
	Action: func(cctx *cli.Context) error {
		if !cctx.Args().Present() {
			return fmt.Errorf("please specify file to make deal for")
		}

		ddir := ddir(cctx)

		mstr := cctx.String("miner")
		if mstr == "" {
			return fmt.Errorf("must specify miner to make deals with")
		}

		miner, err := address.NewFromString(mstr)
		if err != nil {
			return err
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		nd, err := setup(ctx, ddir)
		if err != nil {
			return err
		}

		fc, closer, err := clientFromNode(cctx, nd, ddir)
		if err != nil {
			return err
		}
		defer closer()

		fi, err := os.Open(cctx.Args().First())
		if err != nil {
			return err
		}

		tpr := func(s string, args ...interface{}) {
			fmt.Printf("[%s] "+s+"\n", append([]interface{}{time.Now().Format("15:04:05")}, args...)...)
		}

		bserv := blockservice.New(nd.Blockstore, nil)
		dserv := merkledag.NewDAGService(bserv)

		tpr("importing file...")
		spl := chunker.DefaultSplitter(fi)

		obj, err := importer.BuildDagFromReader(dserv, spl)
		if err != nil {
			return err
		}

		tpr("File CID: %s", obj.Cid())

		ask, err := fc.GetAsk(ctx, miner)
		if err != nil {
			return err
		}

		verified := cctx.Bool("verified")

		price := ask.Ask.Ask.Price
		if verified {
			price = ask.Ask.Ask.VerifiedPrice
		}

		proposal, err := fc.MakeDeal(ctx, miner, obj.Cid(), price, 0, 2880*365, verified)
		if err != nil {
			return err
		}

		propnd, err := cborutil.AsIpld(proposal.DealProposal)
		if err != nil {
			return xerrors.Errorf("failed to compute deal proposal ipld node: %w", err)
		}

		tpr("proposal cid: %s", propnd.Cid())

		if err := saveDealProposal(ddir, propnd.Cid(), proposal.DealProposal); err != nil {
			return err
		}

		resp, err := fc.SendProposal(ctx, proposal)
		if err != nil {
			return err
		}

		tpr("response state: %d", resp.Response.State)
		switch resp.Response.State {
		case storagemarket.StorageDealError:
			return fmt.Errorf("error response from miner: %s", resp.Response.Message)
		case storagemarket.StorageDealProposalRejected:
			return fmt.Errorf("deal rejected by miner: %s", resp.Response.Message)
		default:
			return fmt.Errorf("unrecognized response from miner: %d %s", resp.Response.State, resp.Response.Message)
		case storagemarket.StorageDealWaitingForData, storagemarket.StorageDealProposalAccepted:
			tpr("miner accepted the deal!")
		}

		tpr("starting data transfer... %s", resp.Response.Proposal)

		chanid, err := fc.StartDataTransfer(ctx, miner, resp.Response.Proposal, obj.Cid())
		if err != nil {
			return err
		}

		var lastStatus datatransfer.Status
	loop:
		for {
			status, err := fc.TransferStatus(ctx, chanid)
			if err != nil {
				return err
			}

			switch status.Status {
			case datatransfer.Failed:
				return fmt.Errorf("data transfer failed: %s", status.Message)
			case datatransfer.Cancelled:
				return fmt.Errorf("transfer cancelled: %s", status.Message)
			case datatransfer.Failing:
				tpr("data transfer failing... %s", status.Message)
				// I guess we just wait until its failed all the way?
			case datatransfer.Requested:
				if lastStatus != status.Status {
					tpr("data transfer requested")
				}
				//fmt.Println("transfer is requested, hasnt started yet")
				// probably okay
			case datatransfer.TransferFinished, datatransfer.Finalizing, datatransfer.Completing:
				if lastStatus != status.Status {
					tpr("current state: %s", status.StatusStr)
				}
			case datatransfer.Completed:
				tpr("transfer complete!")
				break loop
			case datatransfer.Ongoing:
				fmt.Printf("[%s] transfer progress: %d      \n", time.Now().Format("15:04:05"), status.Sent)
			default:
				tpr("Unexpected data transfer state: %d (msg = %s)", status.Status, status.Message)
			}
			time.Sleep(time.Millisecond * 100)
			lastStatus = status.Status
		}

		tpr("transfer completed, miner: %s, propcid: %s %s", miner, resp.Response.Proposal, propnd.Cid())

		return nil
	},
}

var infoCmd = &cli.Command{
	Name: "info",
	Action: func(cctx *cli.Context) error {
		ddir := ddir(cctx)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		nd, err := setup(ctx, ddir)
		if err != nil {
			return err
		}

		api, closer, err := lcli.GetGatewayAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		addr, err := nd.Wallet.GetDefault()
		if err != nil {
			return err
		}

		fmt.Println("default client address: ", addr)

		act, err := api.StateGetActor(ctx, addr, types.EmptyTSK)
		if err != nil {
			return err
		}

		fmt.Println("Balance: ", types.FIL(act.Balance))

		pow, err := api.StateVerifiedClientStatus(ctx, addr, types.EmptyTSK)
		if err != nil {
			return err
		}

		fmt.Println("verfied client balance: ", pow)

		return nil
	},
}

var getAskCmd = &cli.Command{
	Name: "get-ask",
	Action: func(cctx *cli.Context) error {
		if !cctx.Args().Present() {
			return fmt.Errorf("please specify miner to query ask of")
		}

		ddir := ddir(cctx)

		miner, err := address.NewFromString(cctx.Args().First())
		if err != nil {
			return err
		}

		fc, closer, err := getClient(cctx, ddir)
		if err != nil {
			return err
		}
		defer closer()

		ask, err := fc.GetAsk(context.TODO(), miner)
		if err != nil {
			return fmt.Errorf("failed to get ask: %s", err)
		}

		fmt.Println("got back ask: ")
		fmt.Println("Miner: ", ask.Ask.Ask.Miner)
		fmt.Println("Price (unverified): ", ask.Ask.Ask.Price)
		fmt.Println("Price (verified): ", ask.Ask.Ask.VerifiedPrice)
		fmt.Println("Min PieceSize: ", ask.Ask.Ask.MinPieceSize)
		fmt.Println("Max PieceSize: ", ask.Ask.Ask.MaxPieceSize)

		return nil
	},
}

var listDealsCmd = &cli.Command{
	Name: "list",
	Action: func(cctx *cli.Context) error {
		ddir := ddir(cctx)

		deals, err := listDeals(ddir)
		if err != nil {
			return err
		}

		for _, dcid := range deals {
			fmt.Println(dcid)
		}

		return nil
	},
}

var retrieveFileCmd = &cli.Command{
	Name: "retrieve",
	Flags: []cli.Flag{
		&cli.StringFlag{Name: "miner", Aliases: []string{"m"}, Required: true},
	},
	Action: func(cctx *cli.Context) error {
		ctx := context.Background()

		cidStr := cctx.Args().First()
		if cidStr == "" {
			return fmt.Errorf("please specify a CID to retrieve")
		}

		minerStr := cctx.String("miner")
		if minerStr == "" {
			return fmt.Errorf("must specify a miner with --miner")
		}

		c, err := cid.Decode(cidStr)
		if err != nil {
			return err
		}

		miner, err := address.NewFromString(minerStr)
		if err != nil {
			return err
		}

		ddir := ddir(cctx)

		fc, closer, err := getClient(cctx, ddir)
		if err != nil {
			return err
		}
		defer closer()

		ask, err := fc.RetrievalQuery(ctx, miner, c)
		if err != nil {
			return err
		}

		proposal, err := retrievehelper.RetrievalProposalForAsk(ask, c, nil)
		if err != nil {
			return err
		}

		stats, err := fc.RetrieveContent(ctx, miner, proposal)
		if err != nil {
			return err
		}

		fmt.Println("retrieved content")
		fmt.Println("Total Payment: ", stats.TotalPayment)
		fmt.Println("Num Payments: ", stats.NumPayments)
		fmt.Println("Size: ", stats.Size)
		fmt.Println("Duration: ", stats.Duration)
		fmt.Println("Average Speed: ", stats.AverageSpeed)
		fmt.Println("Ask Price: ", stats.AskPrice)
		fmt.Println("Peer: ", stats.Peer)

		return nil
	},
}

var queryRetrievalCmd = &cli.Command{
	Name: "query-retrieval",
	Flags: []cli.Flag{
		&cli.StringFlag{Name: "miner", Aliases: []string{"m"}, Required: true},
	},
	Action: func(cctx *cli.Context) error {

		cidStr := cctx.Args().First()
		if cidStr == "" {
			return fmt.Errorf("please specify a CID to query retrieval of")
		}

		minerStr := cctx.String("miner")
		if minerStr == "" {
			return fmt.Errorf("must specify a miner with --miner")
		}

		cid, err := cid.Decode(cidStr)
		if err != nil {
			return err
		}

		miner, err := address.NewFromString(minerStr)
		if err != nil {
			return err
		}

		ddir := ddir(cctx)

		fc, closer, err := getClient(cctx, ddir)
		if err != nil {
			return err
		}
		defer closer()

		query, err := fc.RetrievalQuery(context.TODO(), miner, cid)
		if err != nil {
			return err
		}

		fmt.Println("got retrieval info")
		fmt.Println("Size: ", query.Size)
		fmt.Println("Unseal Price: ", query.UnsealPrice)
		fmt.Println("Min Price Per Byte: ", query.MinPricePerByte)
		fmt.Println("Payment Address: ", query.PaymentAddress)
		if query.Message != "" {
			fmt.Println("Message: ", query.Message)
		}

		return nil
	},
}
