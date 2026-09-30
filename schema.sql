-- =====================================================================
-- News Sentiment Trading Bot - Supabase schema
-- Da eseguire nel SQL Editor di Supabase (idempotente)
-- =====================================================================

-- ---------- ENUM ----------
do $$ begin
    create type log_level as enum ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL');
exception when duplicate_object then null; end $$;

do $$ begin
    create type news_status as enum ('CLAIMED', 'SKIPPED', 'ORDERED', 'FAILED');
exception when duplicate_object then null; end $$;

-- ---------- bot_logs ----------
create table if not exists public.bot_logs (
    id          bigint generated always as identity primary key,
    "timestamp" timestamptz not null default now(),
    level       log_level   not null default 'INFO',
    message     text        not null,
    ticker      varchar(12),
    context     jsonb       not null default '{}'::jsonb,
    run_id      uuid
);
create index if not exists idx_bot_logs_ts     on public.bot_logs ("timestamp" desc);
create index if not exists idx_bot_logs_level  on public.bot_logs (level) where level in ('ERROR','CRITICAL');
create index if not exists idx_bot_logs_ticker on public.bot_logs (ticker);

-- ---------- processed_news ----------
create table if not exists public.processed_news (
    news_id         text primary key,          -- ID notizia Alpaca
    ticker          varchar(12) not null,
    processed_at    timestamptz not null default now(),
    sentiment_score numeric(6,4) check (sentiment_score between -1 and 1),
    status          news_status not null default 'CLAIMED',
    headline        text,
    updated_at      timestamptz not null default now()
);
create index if not exists idx_processed_news_ticker on public.processed_news (ticker, processed_at desc);

-- ---------- active_tickers ----------
create table if not exists public.active_tickers (
    ticker      varchar(12) primary key,
    is_active   boolean not null default true,
    max_notional numeric(12,2) not null default 1000 check (max_notional > 0),
    created_at  timestamptz not null default now()
);

-- ---------- trades (storico transazioni) ----------
create table if not exists public.trades (
    id               bigint generated always as identity primary key,
    news_id          text references public.processed_news(news_id) on delete set null,
    ticker           varchar(12) not null,
    side             varchar(4)  not null check (side in ('buy','sell')),
    qty              numeric(14,4) not null check (qty > 0),
    entry_price      numeric(14,4),
    stop_loss        numeric(14,4),
    take_profit      numeric(14,4),
    alpaca_order_id  text unique,
    status           text not null default 'submitted',
    created_at       timestamptz not null default now()
);
create index if not exists idx_trades_ticker on public.trades (ticker, created_at desc);

-- ---------- trigger updated_at ----------
create or replace function public.touch_updated_at() returns trigger
language plpgsql as $$
begin new.updated_at := now(); return new; end $$;

drop trigger if exists trg_processed_news_touch on public.processed_news;
create trigger trg_processed_news_touch before update on public.processed_news
for each row execute function public.touch_updated_at();

-- ---------- RPC: claim atomico della notizia ----------
-- Ritorna TRUE solo alla prima esecuzione che "prenota" la notizia.
create or replace function public.claim_news(p_news_id text, p_ticker text, p_headline text default null)
returns boolean
language plpgsql
security definer
set search_path = public
as $$
declare v_inserted int;
begin
    insert into public.processed_news (news_id, ticker, headline, status)
    values (p_news_id, upper(p_ticker), p_headline, 'CLAIMED')
    on conflict (news_id) do nothing;
    get diagnostics v_inserted = row_count;
    return v_inserted = 1;
end $$;

revoke all on function public.claim_news(text, text, text) from public, anon, authenticated;
grant execute on function public.claim_news(text, text, text) to service_role;

-- ---------- RLS: nessun accesso pubblico ----------
alter table public.bot_logs       enable row level security;
alter table public.processed_news enable row level security;
alter table public.active_tickers enable row level security;
alter table public.trades         enable row level security;
-- Nessuna policy = anon/authenticated bloccati; service_role bypassa RLS.

-- ---------- Seed di esempio ----------
insert into public.active_tickers (ticker) values ('AAPL'), ('MSFT'), ('NVDA'), ('TSLA')
on conflict (ticker) do nothing;
