package serviceinfo

import "github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws"

// ServiceInfo wraps immutable data from the service.Service structure.
type ServiceInfo struct {
	Config        *aws.Config
	ServiceName   string
	APIVersion    string
	Endpoint      string
	SigningName   string
	SigningRegion string
	JSONVersion   string
	TargetPrefix  string
}
