package store

// This package contains the store handler for NGINX.
// It will be responsible to turn the xDS messages into proper nginx configuration
// and then reload NGINX (until we make it dynamic)
