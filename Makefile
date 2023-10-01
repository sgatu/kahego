build:
	@rm -rf ./dist
	@mkdir ./dist
	cp -r ./config/. ./dist/
	go build -o ./dist/kahego
run:
	@cd dist; GOGC=20 GOMEMLIMIT=350MiB ./kahego