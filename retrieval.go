package main

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/ipfs/go-cid"
	"github.com/whyrusleeping/estuary/filclient"
	"github.com/whyrusleeping/estuary/lib/retrievehelper"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
)

func (s *Server) retrievalAsksForContent(ctx context.Context, contid uint) (map[address.Address]*retrievalmarket.QueryResponse, error) {
	ctx, span := s.tracer.Start(ctx, "retrievalAsksForContent", trace.WithAttributes(
		attribute.Int("content", int(contid)),
	))
	defer span.End()

	var content Content
	if err := s.DB.First(&content, "id = ?", contid).Error; err != nil {
		return nil, err
	}

	var deals []contentDeal
	if err := s.DB.Find(&deals, "content = ? and deal_id > 0", contid).Error; err != nil {
		return nil, err
	}

	fmt.Printf("looking at %d deals for content: %s\n", len(deals), content.Cid.CID)
	out := make(map[address.Address]*retrievalmarket.QueryResponse)
	for _, d := range deals {
		maddr, err := d.MinerAddr()
		if err != nil {
			return nil, err
		}

		resp, err := s.FilClient.RetrievalQuery(ctx, maddr, content.Cid.CID)
		if err != nil {
			s.CM.recordRetrievalFailure(&retrievalFailureRecord{
				Miner:   maddr.String(),
				Phase:   "query",
				Message: err.Error(),
				Content: content.ID,
				Cid:     content.Cid,
			})
			log.Errorf("failed to query miner %s: %s", maddr, err)
			continue
		}

		out[maddr] = resp
	}

	return out, nil
}

type retrievalFailureRecord struct {
	gorm.Model
	Miner   string `json:"miner"`
	Phase   string `json:"phase"`
	Message string `json:"message"`
	Content uint   `json:"content"`
	Cid     dbCID  `json:"cid"`
}

func (cm *ContentManager) recordRetrievalFailure(rfr *retrievalFailureRecord) error {
	return cm.DB.Create(rfr).Error
}

func (s *Server) retrieveContent(ctx context.Context, contid uint) error {
	ctx, span := s.tracer.Start(ctx, "retrieveContent", trace.WithAttributes(
		attribute.Int("content", int(contid)),
	))
	defer span.End()

	content, err := s.CM.getContent(contid)
	if err != nil {
		return err
	}

	asks, err := s.retrievalAsksForContent(ctx, contid)
	if err != nil {
		return err
	}

	fmt.Println("Got asks: ", len(asks))

	if len(asks) == 0 {
		return fmt.Errorf("no retrieval asks for content")
	}

	for m, ask := range asks {
		if err := s.CM.tryRetrieve(ctx, m, content.Cid.CID, ask); err != nil {
			log.Errorw("failed to retrieve content", "miner", m, "content", content.Cid.CID, "err", err)
			s.CM.recordRetrievalFailure(&retrievalFailureRecord{
				Miner:   m.String(),
				Phase:   "retrieval",
				Message: err.Error(),
				Content: contid,
				Cid:     content.Cid,
			})
			continue
		}

		return nil
	}

	return nil
}

func (cm *ContentManager) tryRetrieve(ctx context.Context, maddr address.Address, c cid.Cid, ask *retrievalmarket.QueryResponse) error {

	proposal, err := retrievehelper.RetrievalProposalForAsk(ask, c, nil)
	if err != nil {
		return err
	}

	stats, err := cm.FilClient.RetrieveContent(ctx, maddr, proposal)
	if err != nil {
		return err
	}

	cm.recordRetrievalSuccess(c, maddr, stats)
	return nil
}

type retrievalSuccessRecord struct {
	ID        uint      `gorm:"primarykey" json:"-"`
	CreatedAt time.Time `json:"createdAt"`

	Cid   dbCID  `json:"cid"`
	Miner string `json:"miner"`

	Peer         string `json:"peer"`
	Size         uint64 `json:"size"`
	DurationMs   int64  `json:"durationMs"`
	AverageSpeed uint64 `json:"averageSpeed"`
	TotalPayment string `json:"totalPayment"`
	NumPayments  int    `json:"numPayments"`
	AskPrice     string `json:"askPrice"`
}

func (cm *ContentManager) recordRetrievalSuccess(cc cid.Cid, m address.Address, rstats *filclient.RetrievalStats) {
	if err := cm.DB.Create(&retrievalSuccessRecord{
		Cid:          dbCID{cc},
		Miner:        m.String(),
		Peer:         rstats.Peer.String(),
		Size:         rstats.Size,
		DurationMs:   rstats.Duration.Milliseconds(),
		AverageSpeed: rstats.AverageSpeed,
		TotalPayment: rstats.TotalPayment.String(),
		NumPayments:  rstats.NumPayments,
		AskPrice:     rstats.AskPrice.String(),
	}).Error; err != nil {
		log.Errorf("failed to write retrieval success record: %s", err)
	}
}
