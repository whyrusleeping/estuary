package main

import (
	"context"
	"fmt"

	"github.com/ipfs/go-bitswap"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	ipldformat "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peer"
	rhelp "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/multiformats/go-multiaddr"

	"github.com/cheggaaa/pb/v3"
	cli "github.com/urfave/cli/v2"
)

func main() {
	app := cli.NewApp()

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name: "output",
		},
	}
	app.Action = func(cctx *cli.Context) error {
		if cctx.Args().Len() < 2 {
			return fmt.Errorf("must pass cid and multiaddr of peer to fetch from")
		}

		root, err := cid.Decode(cctx.Args().Get(0))
		if err != nil {
			return err
		}

		maddr, err := multiaddr.NewMultiaddr(cctx.Args().Get(1))
		if err != nil {
			return err
		}
		ai, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			return err
		}

		// set up libp2p node...
		ctx := context.Background()
		h, err := libp2p.New(ctx)
		if err != nil {
			return err
		}

		ds := sync.MutexWrap(datastore.NewMapDatastore())
		bstore := blockstore.NewBlockstore(ds)

		bsnet := bsnet.NewFromIpfsHost(h, &rhelp.Null{})
		bswap := bitswap.New(ctx, bsnet, bstore)

		bserv := blockservice.New(bstore, bswap)
		dag := merkledag.NewDAGService(bserv)

		// connect to our peer
		if err := h.Connect(ctx, *ai); err != nil {
			return fmt.Errorf("failed to connect to target peer: %w", err)
		}

		bar := pb.StartNew(-1)
		bar.Set(pb.Bytes, true)

		cset := cid.NewSet()

		getLinks := func(ctx context.Context, c cid.Cid) ([]*ipldformat.Link, error) {
			if c.Type() == cid.Raw {
				return nil, nil
			}
			node, err := dag.Get(ctx, c)
			if err != nil {
				return nil, err
			}
			bar.Add(len(node.RawData()))

			return node.Links(), nil

		}
		if err := merkledag.Walk(ctx, getLinks, root, cset.Visit, merkledag.Concurrency(8)); err != nil {
			return err
		}

		bar.Finish()

		return nil
	}

	app.RunAndExitOnError()
}
