# emticketen

prototype for `SELECT FOR UPDATE SKIP LOCKED` based ticket sales solution

## getting started

### requirements

* python 3.12+
* hatch (`pipx install hatch`, `brew install hatch` etc.)
* postgresql (uses `PGHOST`, `PGUSER` etc. env vars, see [docs](https://www.postgresql.org/docs/current/libpq-envars.html))

### run tests

currently anything exciting happens in the `tests/test_contested.py` test case that inaccurately simulates thousands of users fighting a battle royale for tickets

```
hatch test -- -s
```

NOTE: uses schema `emticketen_test` for tests, will be wiped if exists
