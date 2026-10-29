import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from data_layer.upstox import auth
from tests.bdd.fake_upstox import FakeUpstox, ThreadedFake, tick_frame
from tests.helpers import ms

scenarios("../features/token_setup.feature")
pytestmark = pytest.mark.bdd

ACCESS, EXTENDED = "ACCESS-TOKEN-VALUE-1234", "EXTENDED-TOKEN-VALUE-5678"


@pytest.fixture()
def envfile(ctx, monkeypatch):
    p = ctx.w.root / ".env"
    p.write_text("FMP_API_KEY=keep-me\nUPSTOX_API_KEY=APPKEY\nUPSTOX_API_SECRET=APPSECRET\n"
                 "UPSTOX_REDIRECT_URI=http://localhost:8000\n")
    monkeypatch.setattr(auth, "ENV_PATH", p)
    for v in ("UPSTOX_API_KEY", "UPSTOX_API_SECRET", "UPSTOX_REDIRECT_URI", "UPSTOX_ACCESS_TOKEN",
              "UPSTOX_EXTENDED_TOKEN", "UPSTOX_FEED_TOKEN", "FMP_API_KEY"):
        monkeypatch.delenv(v, raising=False)
    return p


@given("the Upstox app credentials are in the environment file")
def _creds(envfile, ctx):
    ctx.out = ""


@when("I ask for the login URL")
def _login(ctx, capsys):
    assert auth.main(["login-url"]) == 0
    ctx.out = capsys.readouterr().out


@then("the link points at the Upstox login dialog with my client id and redirect URL")
def _link(ctx):
    assert "https://api.upstox.com/v2/login/authorization/dialog?client_id=APPKEY" in ctx.out
    assert "redirect_uri=http%3A%2F%2Flocalhost%3A8000" in ctx.out


@given("Upstox will exchange a code for an access token and an extended token")
def _exchange_stub(monkeypatch):
    posted = {}

    class R:
        status_code = 200
        def json(self): return {"access_token": ACCESS, "extended_token": EXTENDED}

    def fake_post(url, headers, data, timeout):
        posted.update(url=url, data=data)
        return R()
    monkeypatch.setattr(auth.requests, "post", fake_post)
    return posted


@when("I paste the URL I was redirected to")
def _paste(ctx, capsys):
    auth.main(["exchange", "http://localhost:8000/?code=ONE-TIME-CODE&state=abc"])
    ctx.out = capsys.readouterr().out


@then("the environment file holds both tokens with owner-only permissions")
def _stored(envfile):
    txt = envfile.read_text()
    assert f"UPSTOX_ACCESS_TOKEN={ACCESS}" in txt and f"UPSTOX_EXTENDED_TOKEN={EXTENDED}" in txt
    assert oct(envfile.stat().st_mode & 0o777) == "0o600"


@then("the tokens never appear on screen")
def _hidden(ctx):
    assert ACCESS not in ctx.out and EXTENDED not in ctx.out and "ONE-TIME-CODE" not in ctx.out


@then("my other settings in the environment file are untouched")
def _others(envfile):
    assert "FMP_API_KEY=keep-me" in envfile.read_text()


def start_feed(ctx, reject: set[str]):
    frames = [tick_frame("NSE_EQ|INE000000001", ms(3, 45, 5), 100.0, 5)]
    th = ThreadedFake(FakeUpstox(frames, reject_tokens=reject, hold_s=0.3))
    th.__enter__()
    ctx.th = th
    return th


@pytest.fixture(autouse=True)
def _close_server(ctx):
    yield
    if getattr(ctx, "th", None):
        ctx.th.__exit__()


@given("an Upstox feed that accepts only the extended token")
def _only_ext(ctx, envfile, monkeypatch):
    ctx.w.set_universe({"IN": ["A.NS"]})
    start_feed(ctx, {ACCESS})
    monkeypatch.setenv("UPSTOX_ACCESS_TOKEN", ACCESS); monkeypatch.setenv("UPSTOX_EXTENDED_TOKEN", EXTENDED)


@given("an Upstox feed that accepts only the access token")
def _only_access(ctx, envfile, monkeypatch):
    ctx.w.set_universe({"IN": ["A.NS"]})
    start_feed(ctx, {EXTENDED})
    monkeypatch.setenv("UPSTOX_ACCESS_TOKEN", ACCESS); monkeypatch.setenv("UPSTOX_EXTENDED_TOKEN", EXTENDED)


@when("I check which token the feed accepts and adopt the extended one")
def _check(ctx, capsys):
    ctx.code = auth.cmd_check(adopt=True, url=ctx.th.url)
    ctx.out = capsys.readouterr().out


@then("the extended token is saved as the feed token")
def _adopted(ctx, envfile):
    assert ctx.code == 0 and f"UPSTOX_FEED_TOKEN={EXTENDED}" in envfile.read_text()


@then("the collector will prefer the feed token")
def _prefer(envfile, monkeypatch):
    from dotenv import dotenv_values
    v = dotenv_values(envfile)
    chosen = v.get("UPSTOX_FEED_TOKEN") or v.get("UPSTOX_ACCESS_TOKEN")
    assert chosen == EXTENDED


@then("nothing is adopted")
def _not_adopted(ctx, envfile):
    assert ctx.code == 1 and "UPSTOX_FEED_TOKEN" not in envfile.read_text()


@then("I am told the access token must be refreshed daily")
def _told(ctx):
    assert "refreshed daily" in ctx.out and ACCESS not in ctx.out
