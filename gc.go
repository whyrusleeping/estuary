package main

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

func (cm *ContentManager) GarbageCollect(ctx context.Context) error {
	// since we're reference counting all the content, garbage collection becomes easy
	// its even easier if we don't care that its 'perfect'

	// We can probably even just remove stuff when its references are removed from the database
	keych, err := cm.Blockstore.AllKeysChan(ctx)
	if err != nil {
		return err
	}

	for c := range keych {
		_, err := cm.maybeRemoveObject(c)
		if err != nil {
			return err
		}
	}

	return nil
}

func (cm *ContentManager) maybeRemoveObject(c cid.Cid) (bool, error) {
	cm.contentLk.Lock()
	defer cm.contentLk.Unlock()
	keep, err := cm.trackingObject(c)
	if err != nil {
		return false, err
	}

	if !keep {
		// can batch these deletes and execute them at the datastore layer for more perfs
		if err := cm.Blockstore.DeleteBlock(c); err != nil {
			return false, err
		}

		return true, nil
	}

	return false, nil
}

func (cm *ContentManager) trackingObject(c cid.Cid) (bool, error) {
	var count int64
	if err := cm.DB.Model(&Object{}).Where("cid = ?", c.Bytes()).Count(&count).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}

	return count > 0, nil
}

func (cm *ContentManager) RemoveContent(ctx context.Context, c uint, now bool) error {
	ctx, span := cm.tracer.Start(ctx, "RemoveContent")
	defer span.End()

	cm.contentLk.Lock()
	defer cm.contentLk.Unlock()

	if err := cm.DB.Delete(&Content{}, c).Error; err != nil {
		return fmt.Errorf("failed to delete content from db: %w", err)
	}

	var objIds []struct {
		Object uint
	}

	if err := cm.DB.Model(&ObjRef{}).Find(&objIds, "content = ?", c).Error; err != nil {
		return fmt.Errorf("failed to gather referenced object IDs: %w", err)
	}

	if err := cm.DB.Where("content = ?", c).Delete(&ObjRef{}).Error; err != nil {
		return fmt.Errorf("failed to delete related object references: %w", err)
	}

	ids := make([]uint, len(objIds))
	for i, obj := range objIds {
		ids[i] = obj.Object
	}

	// Since im kinda bad at sql, this is going to be faster than the naive
	// query for now. Maybe can think of something more clever later
	batchSize := 100
	for i := 0; i < len(ids); i += 100 {
		count := batchSize
		if len(ids[i:]) < count {
			count = len(ids[i:])
		}

		slice := ids[i : i+count]

		subq := cm.DB.Table("obj_refs").Select("1").Where("obj_refs.object = objects.id")
		if err := cm.DB.Where("id IN ? and not exists (?)", slice, subq).Delete(&Object{}).Error; err != nil {
			return err
		}
	}

	if !now {
		return nil
	}

	// TODO: copied from the offloading method, need to refactor this into something better
	q := cm.DB.Debug().Model(&ObjRef{}).
		Select("cid").
		Joins("left join objects on obj_refs.object = objects.id").
		Group("cid").
		Having("MIN(obj_refs.offloaded) = 1")

	rows, err := q.Rows()
	if err != nil {
		return err
	}

	for rows.Next() {
		var dbc dbCID
		if err := rows.Scan(&dbc); err != nil {
			return err
		}

		if err := cm.Blockstore.DeleteBlock(dbc.CID); err != nil {
			return err
		}
	}

	return nil
}
