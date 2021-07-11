package retrievehelper

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	exchangeoffline "github.com/ipfs/go-ipfs-exchange-offline"
	mdagipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"golang.org/x/xerrors"

	// must be imported to init() raw-codec support
	_ "github.com/ipld/go-ipld-prime/codec/raw"

	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	ipldbasicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorbuilder "github.com/ipld/go-ipld-prime/traversal/selector/builder"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
)

var ssb = selectorbuilder.NewSelectorSpecBuilder(ipldbasicnode.Prototype.Any)
var RecurseAllSelectorBuilder = ssb.ExploreRecursive(
	selector.RecursionLimitNone(),
	ssb.ExploreUnion(
		ssb.Matcher(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
	),
)

func ResolvePath(ctx context.Context, bs blockstore.Blockstore, startCid cid.Cid, path textselector.Expression) (cid.Cid, error) {

	selSpec, err := textselector.SelectorSpecFromPath(path, nil)
	if err != nil {
		return cid.Undef, err
	}

	var subDagFound bool

	if err := TraverseDag(
		ctx,
		merkledag.NewDAGService(blockservice.New(bs, exchangeoffline.Exchange(bs))),
		startCid,
		selSpec.Node(),
		func(p traversal.Progress, n ipld.Node, r traversal.VisitReason) error {
			if r == traversal.VisitReason_SelectionMatch {
				cidLnk, castOK := p.LastBlock.Link.(cidlink.Link)
				if !castOK {
					return xerrors.Errorf("cidlink cast unexpectedly failed on '%s'", p.LastBlock.Link.String())
				}
				startCid = cidLnk.Cid
				subDagFound = true
				return traversal.SkipMe{}
			}
			return nil
		},
	); err != nil {
		return cid.Undef, err
	}
	if !subDagFound {
		return cid.Undef, xerrors.Errorf("path selection '%s' does not match a node within %s", path, startCid)
	}
	return startCid, nil
}

func TraverseDag(
	ctx context.Context,
	ds mdagipld.DAGService,
	startFrom cid.Cid,
	optionalSelector ipld.Node,
	visitCallback traversal.AdvVisitFn,
) error {

	// If no selector is given - use *.*
	// See discusion at https://github.com/ipld/go-ipld-prime/issues/171
	if optionalSelector == nil {
		optionalSelector = RecurseAllSelectorBuilder.Node()
	}

	parsedSelector, err := selector.ParseSelector(optionalSelector)
	if err != nil {
		return err
	}

	// not sure what this is for TBH...
	linkContext := ipld.LinkContext{Ctx: ctx}

	// this is what allows us to understand dagpb
	nodePrototypeChooser := dagpb.AddSupportToChooser(
		func(ipld.Link, ipld.LinkContext) (ipld.NodePrototype, error) {
			return ipldbasicnode.Prototype.Any, nil
		},
	)

	// this is how we implement GETs
	linkSystem := cidlink.DefaultLinkSystem()
	linkSystem.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		cl, isCid := lnk.(cidlink.Link)
		if !isCid {
			return nil, fmt.Errorf("unexpected link type %#v", lnk)
		}

		node, err := ds.Get(lctx.Ctx, cl.Cid)
		if err != nil {
			return nil, err
		}

		return bytes.NewBuffer(node.RawData()), nil
	}

	// this is how we pull the start node out of the DS
	startLink := cidlink.Link{Cid: startFrom}
	startNodePrototype, err := nodePrototypeChooser(startLink, linkContext)
	if err != nil {
		return err
	}
	startNode, err := linkSystem.Load(
		linkContext,
		startLink,
		startNodePrototype,
	)
	if err != nil {
		return err
	}

	// this is the actual execution, invoking the supplied callback
	return traversal.Progress{
		Cfg: &traversal.Config{
			Ctx:                            ctx,
			LinkSystem:                     linkSystem,
			LinkTargetNodePrototypeChooser: nodePrototypeChooser,
		},
	}.WalkAdv(startNode, parsedSelector, visitCallback)
}
