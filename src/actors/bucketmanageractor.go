package actors

import (
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
	"sgatu.com/kahego/src/config"
	"sgatu.com/kahego/src/streams"
)

type BucketManagerActor struct {
	Actor
	WaitableActor
	OrderedMessagesActor
	bucketActors        map[string]Actor
	BucketsConfig       map[string]config.BucketConfig
	DefaultBucketConfig *config.BucketConfig
	waitGroupBuckets    *sync.WaitGroup
}

func (dbma *BucketManagerActor) getBucket(bucket string) (Actor, error) {
	if bucket, ok := dbma.bucketActors[bucket]; ok {
		return bucket, nil
	}
	var config *config.BucketConfig = nil
	if bucketConfig, ok := dbma.BucketsConfig[bucket]; ok {
		config = &bucketConfig
	} else {
		config = dbma.DefaultBucketConfig
	}
	if config != nil {
		bucketActor := &BucketActor{
			Actor:           &BaseActor{},
			WaitableActor:   &BaseWaitableActor{WaitGroup: dbma.waitGroupBuckets},
			SupervisedActor: &BaseSupervisedActor{supervisor: dbma, id: bucket},
			BucketConfig:    *config,
			BucketId:        bucket,
		}
		dbma.bucketActors[bucket] = bucketActor
		InitializeAndStart(bucketActor)
		return bucketActor, nil
	}
	return nil, fmt.Errorf("no configuration for bucket %s available, neither a default one was defined", bucket)
}
func (dbma *BucketManagerActor) DoWork(msg interface{}) (WorkResult, error) {

	switch msg := msg.(type) {
	case *streams.Message:
		bucket, err := dbma.getBucket(msg.Bucket)
		if err == nil {
			Tell(bucket, msg)
		} else {
			fmt.Println(err)
		}
	case PoisonPill:
		log.Trace("BucketManagerActor received a PosionPill")
		return Stop, nil
	case IllChildMessage:
		log.Warn(fmt.Sprintf("Bucket %s actor has died due to %s", msg.Id, msg.Error))
		delete(dbma.bucketActors, msg.Id)
	default:
		log.Warn(fmt.Sprintf("Bucket Manager Actor received invalid message %T", msg))
	}
	return Continue, nil
}

// override GetWorkMethod
func (dbma *BucketManagerActor) GetWorkMethod() DoWorkMethod {
	return dbma.DoWork
}

func (dbma *BucketManagerActor) OnStart() error {
	dbma.bucketActors = make(map[string]Actor)
	dbma.waitGroupBuckets = &sync.WaitGroup{}
	return nil
}
func (dbma *BucketManagerActor) OnStop() error {
	for _, bucket := range dbma.bucketActors {
		Tell(bucket, PoisonPill{})
	}
	dbma.waitGroupBuckets.Wait()
	return nil
}
