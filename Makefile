.PHONY: openlist init seed smoke

openlist:
	nix develop --command OpenList server --data ./openlist_data

init:
	@cp -n pass.example.txt pass.txt 2>/dev/null || true
	@cp -n routes.example.json routes.json 2>/dev/null || true

seed: init
	nix develop --command python scripts/seed_openlist_testdata.py

smoke: init
	nix develop --command uv run python scripts/smoke_openlist_zero_trust.py
