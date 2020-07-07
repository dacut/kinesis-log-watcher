# Kinesis Log Watcher
Watch Kinesis logs in real time.

## Usage
<code>kinesis-log-watcher [<i>options</i>] _stream-name_</code>

Watch incoming log entries from a Kinesis stream. This is intended to be a
companion to kinesis-log-streamer.

Valid durations are a number followed by a unit abbreviation.
Examples: 30s, 5m, 3h, 1d.

The format string uses the [Go template format](https://golang.org/pkg/text/template/). Fields available are:

* `{{.HostId}}` `{{.HostID}}` -- The full ARN of the host generating the log.
* `{{.ShortHostId}}` `{{.ShortHostID}}` -- Short hostname (just the last part).
* `{{.Timestamp}}` -- The timestamp when the log was sent to Kinesis.
* `{{.LogEntry}}` -- The log entry in string format.
* `{{.Log}}` -- If the log entry could be parsed as JSON, the resulting JSON structure. You can get embedded fields using `{{.Log.FieldName}}`.

To format the timestamp in different formats (e.g. ISO 8601), you can use either:

* `{{.Timestamp Format "2005-01-02T15:04:05Z"}}` or
* `{{strftime "%Y-%m-%dT%H:%M:%S" .Timestamp}}`

## Options

* <code>-f _template_ | --format=_template_</code>  
  Format template for log entries. Default is `{{.ShortHostId}} {{.Timestamp}} {{.LogEntry}}`
  
* <code>-h | --help</code>  
  Show this usage information.

* <code>-O | --one-shot</code>  
  Display logs only once; don't poll.

* <code>-p _profile_ | --profile=_profile_</code>  
  If specified, obtain AWS credentials from the specified profile in ~/.aws/credentials.

* <code>-r _region_ | --region=_region_</code>  
  The AWS region to use. If unspecified, the value from the `$AWS_REGION` environment variable is used.

* <code>-s _duration_ | --start=_duration_</code>  
  Start time to start polling from, specified as a duration from the current time. Default is `5m`.

* <code>-w _duration_ | --watch=_duration_</code>  
  Watch/poll time, specified as a duration. Default is `10s`.

## License

This program and associated documentation and source code are licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
