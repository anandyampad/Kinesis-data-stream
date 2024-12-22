package consumer

import (
	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go/aws"
	cfg "github.com/vmware/vmware-go-kcl-v2/clientlibrary/config"
	kc "github.com/vmware/vmware-go-kcl-v2/clientlibrary/interfaces"
	wk "github.com/vmware/vmware-go-kcl-v2/clientlibrary/worker"
)

const streamName = "MyLocalKinesisStream" // The name of the Kinesis stream

func createWorker(creds credentials.StaticCredentialsProvider, workerID string) *wk.Worker {
	kclConfig := cfg.NewKinesisClientLibConfigWithCredentials("EventProcessor", "MyLocalKinesisStream", "us-east-1", workerID, &creds, &creds).
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithKinesisEndpoint("http://localhost:4566").
		WithDynamoDBEndpoint("http://localhost:4566")

	return wk.NewWorker(recordProcessorFactory(), kclConfig)

}

func InitConsumer(args []string) {
	//log.SetLevel(log.DebugLevel)
	creds := credentials.NewStaticCredentialsProvider("test", "test", "")

	worker1 := createWorker(creds, args[0])
	worker2 := createWorker(creds, args[1])
	worker3 := createWorker(creds, args[2])
	worker4 := createWorker(creds, args[3])

	// Start the workers in a goroutine so that it can process records
	go worker1.Start()
	go worker2.Start()
	go worker3.Start()
	go worker4.Start()

}

// Record processor factory is used to create RecordProcessor
func recordProcessorFactory() kc.IRecordProcessorFactory {
	return &dumpRecordProcessorFactory{}
}

// Simple record processor and dump everything
type dumpRecordProcessorFactory struct{}

func (d *dumpRecordProcessorFactory) CreateProcessor() kc.IRecordProcessor {
	return &dumpRecordProcessor{}
}

// Create a dump record processor for printing out all data from the record.
type dumpRecordProcessor struct{}

func (dd *dumpRecordProcessor) Initialize(input *kc.InitializationInput) {
	log.Printf("Processing ShardId: %v at checkpoint: %v", input.ShardId, aws.StringValue(input.ExtendedSequenceNumber.SequenceNumber))
}

func (dd *dumpRecordProcessor) ProcessRecords(input *kc.ProcessRecordsInput) {
	log.Print("Processing Records...")

	// Don't process empty records
	if len(input.Records) == 0 {
		return
	}

	for _, v := range input.Records {
		log.Printf("Record = %s", v.Data)
	}

	// Checkpoint after processing this batch
	lastRecordSequenceNumber := input.Records[len(input.Records)-1].SequenceNumber
	log.Printf("Checkpoint progress at: %v, MillisBehindLatest = %v", lastRecordSequenceNumber, input.MillisBehindLatest)
	input.Checkpointer.Checkpoint(lastRecordSequenceNumber)
}

func (dd *dumpRecordProcessor) Shutdown(input *kc.ShutdownInput) {
	log.Printf("Shutdown Reason: %v", aws.StringValue(kc.ShutdownReasonMessage(input.ShutdownReason)))

	// If termination occurs, checkpoint to avoid errors
	if input.ShutdownReason == kc.TERMINATE {
		input.Checkpointer.Checkpoint(nil)
	}
}
