MOCHA_OPTS= --slow 0 -A
REPORTER = spec

lint-fix:
	./node_modules/.bin/eslint --fix

lint-check:
	./node_modules/.bin/eslint

commit:
	@node ./node_modules/ac-semantic-release/lib/commit.js

release:
	@node ./node_modules/ac-semantic-release/lib/release.js

test-release:
	DEBUGMODE=true node ./node_modules/ac-semantic-release/lib/release.js
	
.PHONY: check
