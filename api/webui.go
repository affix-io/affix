package api

import (
	"net/http"
)

// WebuiHandler returns the webui html
func WebuiHandler(w http.ResponseWriter, r *http.Request) {
	template := `
<!doctype html>
<html>
	<head>
		<link href="https://storage.googleapis.com/affix-static/bundle.css" rel="stylesheet">
		<title>affix Remote Webapp</title><meta charset="utf-8">
		<meta http-equiv="Content-Security-Policy" content="script-src 'self' https://storage.googleapis.com https://affix.io https://affix.cloud">
	</head>
	<body>
		<div class="titlebar"></div>
		<div id="root"></div>
		<script src="https://storage.googleapis.com/affix-static/bundle.js"></script>
	</body>
</html>
	`
	w.Write([]byte(template))
	return
}
