package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/lestrrat-go/strftime"
	getopt "github.com/pborman/getopt/v2"
)

const maxBackoff = time.Duration(10 * time.Second)
const maxSleepWithoutSequence = time.Duration(60 * time.Second)

// WriteUsageToStderr print usage information to os.Stderr. This is provided
// for signature compatibility is getopt.SetUsage.
func WriteUsageToStderr() {
	WriteUsage(os.Stderr)
}

// WriteUsage prints usage information to the specified Writer.
func WriteUsage(w io.Writer) {
	fmt.Fprintf(w, `kinesis-log-watcher [options] <stream-name>

Watch incoming log entries from a Kinesis stream. This is intended to be a
companion to kinesis-log-streamer.

Valid durations are a number followed by a unit abbreviation.
Examples: 30s, 5m, 3h, 1d.

The format string uses the Go template format; full documentation is available
at https://golang.org/pkg/text/template/. Fields available are:
    {{.HostId}} {{.HostID}} -- The full ARN of the host generating the log.
    {{.ShortHostId}} {{.ShortHostID}} -- Short hostname (just the last part).
    {{.Timestamp}} -- The timestamp when the log was sent to Kinesis.
    {{.LogEntry}} -- The log entry in string format.
    {{.Log}} -- If the log entry could be parsed as JSON, the resulting JSON
        structure. You can get embedded fields using {{.Log.FieldName}}.

To format the timestamp in different formats (e.g. ISO 8601), you can use either:
    {{.Timestamp Format "2005-01-02T15:04:05Z"}} or
    {{strftime "%%Y-%%m-%%dT%%H:%%M:%%S" .Timestamp}}

`)

	getopt.PrintUsage(w)
}

func main() {
	getopt.BoolLong("help", 'h', "Show this usage information.")
	getopt.BoolLong("one-shot", 'O', "Display logs only once.")
	getopt.StringLong("format", 'f', `{{.ShortHostId}} {{.Timestamp}} {{.LogEntry}}`, "Format template for log entries.", "<template>")
	getopt.StringLong("start", 's', "5m", "Start time to start polling from, specified as a duration from the current time.", "<duration>")
	getopt.StringLong("watch", 'w', "10s", "Watch/poll time, specified as a duration.", "<duration>")
	getopt.StringLong("region", 'r', "", "The AWS region to use. If unspecified, the value from the $AWS_REGION environment variable is used.", "<region>")
	getopt.StringLong("profile", 'p', "", "If specified, obtain AWS credentials from the specified profile in ~/.aws/credentials.", "<profile>")
	getopt.SetUsage(WriteUsageToStderr)

	getopt.Parse()

	if getopt.GetCount('h') > 0 {
		WriteUsage(os.Stdout)
		os.Exit(0)
	}

	format := getopt.GetValue('f')

	startTimeStr := getopt.GetValue('s')
	startTimeDuration, err := time.ParseDuration(startTimeStr)
	if err != nil || startTimeDuration < 0 {
		fmt.Fprintf(os.Stderr, "Invalid start time: %s\n", startTimeStr)
		WriteUsageToStderr()
		os.Exit(2)
	}

	oneShot := bool(getopt.GetCount('O') > 0)
	var pollTime time.Duration
	if oneShot {
		pollTime = math.MaxInt64
	} else {
		pollTimeStr := getopt.GetValue('w')
		pollTime, err = time.ParseDuration(pollTimeStr)
		if err != nil || pollTime <= 0 {
			fmt.Fprintf(os.Stderr, "Invalid watch/poll time: %s\n", pollTimeStr)
			WriteUsageToStderr()
			os.Exit(2)
		}
	}

	formatTemplate := template.New("format")
	// Register template functions
	templateFuncs := make(template.FuncMap)
	templateFuncs["strftime"] = templateStrftime
	formatTemplate = formatTemplate.Funcs(templateFuncs)

	formatTemplate, err = formatTemplate.Parse(format)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid format: %s: %v\n", format, err)
		WriteUsageToStderr()
		os.Exit(2)
	}

	// Make sure we have a stream name.
	args := getopt.Args()

	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Kinesis stream must be specified.\n")
		WriteUsageToStderr()
		os.Exit(2)
	}

	if len(args) > 1 {
		fmt.Fprintf(os.Stderr, "Unknown argument: %s\n", args[1])
		WriteUsageToStderr()
		os.Exit(2)
	}

	kinesisStreamName := args[0]

	region := getopt.GetValue('r')
	profile := getopt.GetValue('p')

	awsSessionOptions := session.Options{Profile: profile}
	if region != "" {
		awsSessionOptions.Config.Region = aws.String(region)
	}

	awsSession, err := session.NewSessionWithOptions(awsSessionOptions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create an AWS session: %v\n", err)
		os.Exit(1)
	}

	kinesisClient := kinesis.New(awsSession)
	listShardsInput := kinesis.ListShardsInput{StreamName: aws.String(kinesisStreamName)}
	shardIDs := make(map[string]bool)
	closeRequestedChan := make(chan bool, 16)
	doneChan := make(chan string, 16)

	for {
		output, err := kinesisClient.ListShards(&listShardsInput)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to list shards for Kinesis stream %s: %v\n", kinesisStreamName, err)
			os.Exit(1)
		}

		for _, shard := range output.Shards {
			shardID := aws.StringValue(shard.ShardId)
			shardIDs[shardID] = true
		}

		if output.NextToken == nil {
			break
		}

		// More shards to list
		listShardsInput.StreamName = nil
		listShardsInput.NextToken = output.NextToken
	}

	startTime := time.Now().UTC().Add(-startTimeDuration)

	for shardID := range shardIDs {
		go processShard(kinesisClient, kinesisStreamName, shardID, startTime, pollTime, formatTemplate, closeRequestedChan, doneChan)
	}

	interruptChan := make(chan os.Signal)
	closeRequested := false

	// Keep running until we're interrupted.
	if !oneShot {
		signal.Notify(interruptChan, syscall.SIGINT)
	} else {
		close(closeRequestedChan)
		closeRequested = true
	}

	for len(shardIDs) > 0 {
		select {
		case <-interruptChan:
			if !closeRequested {
				fmt.Fprintf(os.Stderr, "Ctrl+C received; stopping all shard processors.\n")
				close(closeRequestedChan)
				closeRequested = true
			}

		case shardID := <-doneChan:
			_, present := shardIDs[shardID]
			if !present {
				fmt.Fprintf(os.Stderr, "Got spurious done response from %s\n", shardID)
			} else {
				delete(shardIDs, shardID)
			}
		}
	}

	os.Exit(0)
}

func signalDone(shardID string, doneChan chan string) {
	doneChan <- shardID
}

func getRecords(kinesisClient *kinesis.Kinesis, shardIterator string) (*kinesis.GetRecordsOutput, error) {
	gri := kinesis.GetRecordsInput{ShardIterator: aws.String(shardIterator), Limit: aws.Int64(10000)}
	backoff := time.Duration(50 * time.Millisecond)

	for {
		gro, err := kinesisClient.GetRecords(&gri)
		if err == nil {
			return gro, nil
		}

		// Is this a throttling exception? If so, back off and retry.
		if awsErr, castOk := err.(awserr.Error); castOk {
			errCode := awsErr.Code()
			if errCode == "ProvisionedThroughputExceededException" || errCode == "KMSThrottlingException" {
				time.Sleep(backoff)
				backoff = (3 * backoff) / 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
		}

		// Nope, propagate the error.
		return nil, err
	}
}

var formatErrorPrinted uint32 = 0

func printRecords(records []*kinesis.Record, formatTemplate *template.Template) {
	for _, record := range records {
		// The partition key is actually the host ID, in ARN format or ip-address:a.b.c.d or uuid:...
		hostID := aws.StringValue(record.PartitionKey)

		// Compute a short host ID that eliminates most of the ARN garbage
		hostIDParts := strings.Split(hostID, ":")
		shortHostID := hostIDParts[len(hostIDParts)-1]

		if strings.HasPrefix(shortHostID, "task/") {
			// For ECS tasks, remove the task/ prefix.
			shortHostID = shortHostID[5:]
		} else if strings.HasPrefix(shortHostID, "instance/") {
			// for EC2 instances, remove the instance/ prefix.
			shortHostID = shortHostID[9:]
		}

		logData := make(map[string]interface{})
		logData["Timestamp"] = aws.TimeValue(record.ApproximateArrivalTimestamp)
		logData["HostId"] = hostID
		logData["HostID"] = hostID
		logData["ShortHostId"] = shortHostID
		logData["ShortHostID"] = shortHostID
		logData["SequenceNumber"] = aws.StringValue(record.SequenceNumber)
		logData["LogEntry"] = string(record.Data)

		var jsonData interface{}
		err := json.Unmarshal(record.Data, &jsonData)
		if err != nil {
			logData["Log"] = nil
		} else {
			logData["Log"] = jsonData
		}

		lineBuffer := new(bytes.Buffer)
		err = formatTemplate.Execute(lineBuffer, logData)
		if err != nil {
			if atomic.CompareAndSwapUint32(&formatErrorPrinted, 0, 1) {
				fmt.Fprintf(os.Stderr, "Template formatting error: %v\n", err)
			}
		} else {
			fmt.Println(string(lineBuffer.Bytes()))
		}
	}

	os.Stdout.Sync()
}

func processShard(kinesisClient *kinesis.Kinesis, streamName string, shardID string, startTime time.Time, pollTime time.Duration, formatTemplate *template.Template, closeRequestedChan chan bool, doneChan chan string) {
	// Send a done signal for this shard upon return
	defer signalDone(shardID, doneChan)

	// Get the first shard iterator, starting at the specified start time.
	gsii := kinesis.GetShardIteratorInput{
		StreamName:        aws.String(streamName),
		ShardId:           aws.String(shardID),
		ShardIteratorType: aws.String("AT_TIMESTAMP"),
		Timestamp:         aws.Time(startTime),
	}

	gsio, err := kinesisClient.GetShardIterator(&gsii)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to get initial shard iterator for stream %s shard %s: %v\n", streamName, shardID, err)
		return
	}

	shardIterator := aws.StringValue(gsio.ShardIterator)
	if shardIterator == "" {
		fmt.Fprintf(os.Stderr, "Unable to get initial shard iterator for stream %s shard %s: empty shard iterator\n", streamName, shardID)
		return
	}

	// Loop over log entries until we've been requested to exit.
	latestSequenceNumber := ""
	for {
		var gro *kinesis.GetRecordsOutput
		var err error

		// If this isn't the first we're reading logs (e.g., after we've paused), we may end up with an expired shard iterator
		for retries := 0; retries < 2; retries++ {
			gro, err = getRecords(kinesisClient, shardIterator)
			if err == nil {
				break
			}

			if awsErr, castOk := err.(awserr.Error); retries == 0 && castOk {
				if awsErr.Code() == "ExpiredIteratorException" && latestSequenceNumber != "" {
					// Get a new shard iterator.
					gsii.ShardIteratorType = aws.String("AT_SEQUENCE_NUMBER")
					gsii.StartingSequenceNumber = aws.String(latestSequenceNumber)
					gsii.Timestamp = nil

					gsio, err = kinesisClient.GetShardIterator(&gsii)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Unable to renew shard iterator for stream %s shard %s: %v\n", streamName, shardID, err)
						return
					}

					shardIterator := aws.StringValue(gsio.ShardIterator)
					if shardIterator == "" {
						fmt.Fprintf(os.Stderr, "Unable to renew initial shard iterator for stream %s shard %s: empty shard iterator\n", streamName, shardID)
						return
					}
					continue
				}
			}

			fmt.Fprintf(os.Stderr, "Unable to get initial records for stream %s shard %s: %v\n", streamName, shardID, err)
			return
		}

		if len(gro.Records) > 0 {
			printRecords(gro.Records, formatTemplate)

			// Grab the sequence number from the last record
			latestSequenceNumber = aws.StringValue(gro.Records[len(gro.Records)-1].SequenceNumber)
		}

		if aws.Int64Value(gro.MillisBehindLatest) == 0 {
			// We're caught up.
			currentPollTime := pollTime
			if latestSequenceNumber == "" {
				// We don't have a sequence number to grab a new shard iterator; don't sleep for longer than 1 minute
				if currentPollTime > maxSleepWithoutSequence {
					currentPollTime = maxSleepWithoutSequence
				}
			}

			// Pause until we've been requested to exit or our poll timer expires.
			select {
			case <-closeRequestedChan:
				return

			case <-time.After(currentPollTime):
				// Just fall through.
			}
		} else {
			// Stop if we've been asked to exit.
			select {
			case <-closeRequestedChan:
				return

			default:
				// Just fall through.
			}
			// We're not caught up; keep going.
			shardIterator = aws.StringValue(gro.NextShardIterator)
		}
	}
}

var invalidTimestampFormatErrorPrinted uint32 = 0

func templateStrftime(timeFormat string, timeGeneric interface{}) string {
	timeProper, castOk := timeGeneric.(time.Time)
	if !castOk {
		return ""
	}

	result, err := strftime.Format(timeFormat, timeProper, strftime.WithMicroseconds('f'), strftime.WithMilliseconds('L'), strftime.WithUnixSeconds('s'))
	if err == nil {
		return result
	}

	if atomic.CompareAndSwapUint32(&invalidTimestampFormatErrorPrinted, 0, 1) {
		fmt.Fprintf(os.Stderr, "Invalid timestamp format: %s. Using RFC3339Nano instead.\n", timeFormat)
	}

	return timeProper.Format(time.RFC3339Nano)
}
