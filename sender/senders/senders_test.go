package senders

import "fmt"

func ExampleRemoteServerSettings_GenerateHTTPSURL() {
	rss := RemoteServerSettings{Protocol: "https", Hostname: "example.com", Port: 443, Path: "/testpath"}
	fmt.Println(rss.GenerateHTTPSURL())
	// Output: https://example.com:443/testpath
}

func ExampleRemoteServerSettings_GenerateHTTPURL() {
	rss := RemoteServerSettings{Protocol: "http", Hostname: "example.com", Port: 443, Path: "/testpath"}
	fmt.Println(rss.GenerateHTTPURL())
	// Output: http://example.com:443/testpath
}

func ExampleRemoteServerSettings_GenerateTCP() {
	rss := RemoteServerSettings{Protocol: "tcp", Hostname: "example.com", Port: 8080}
	fmt.Println(rss.GenerateTCP())
	// Output: example.com:8080
}
