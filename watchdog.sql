-- =====================================================================
-- Watchdog: avvisa su Telegram se il bot non dà segnali da oltre 15 minuti.
-- Gira dentro Supabase (pg_cron + pg_net), quindi funziona anche se il Raspberry
-- è spento o senza rete. Da eseguire UNA VOLTA nel SQL Editor, dopo schema.sql
-- e dopo aver salvato TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID nel Vault.
--
-- Il messaggio "bot di nuovo online" lo invia il bot stesso alla ripartenza.
-- Per disattivare:  select cron.unschedule('bot-watchdog');
-- =====================================================================

create extension if not exists pg_net;
create extension if not exists pg_cron;

create or replace function public.watchdog_check(p_max_silence interval default interval '15 minutes')
returns void
language plpgsql
security definer
set search_path = ''
as $$
declare
    v_last    timestamptz;
    v_alerted boolean;
    v_token   text;
    v_chat    text;
    v_minutes int;
begin
    select h.last_seen, h.down_alerted into v_last, v_alerted
    from public.bot_heartbeat h where h.id = 1;

    -- Nessun segnale mai ricevuto, avviso già inviato o bot in regola: niente da fare
    if v_last is null or v_alerted or now() - v_last <= p_max_silence then
        return;
    end if;

    select ds.decrypted_secret into v_token from vault.decrypted_secrets ds where ds.name = 'TELEGRAM_BOT_TOKEN' limit 1;
    select ds.decrypted_secret into v_chat  from vault.decrypted_secrets ds where ds.name = 'TELEGRAM_CHAT_ID' limit 1;
    if v_token is null or v_chat is null then
        return;
    end if;

    v_minutes := floor(extract(epoch from (now() - v_last)) / 60);

    perform net.http_post(
        url     := 'https://api.telegram.org/bot' || v_token || '/sendMessage',
        body    := jsonb_build_object(
                       'chat_id', v_chat,
                       'parse_mode', 'HTML',
                       'text', '<b>Il bot non risponde</b>' || chr(10) ||
                               'Ultimo segnale: ' || to_char(v_last at time zone 'Europe/Rome', 'DD/MM HH24:MI') ||
                               ' (' || v_minutes || ' minuti fa)' || chr(10) ||
                               'Controlla il Raspberry: alimentazione, rete e servizio trading-bot.'),
        headers := '{"Content-Type": "application/json"}'::jsonb
    );

    update public.bot_heartbeat set down_alerted = true where id = 1;
end $$;

revoke all on function public.watchdog_check(interval) from public, anon, authenticated;

-- Controllo ogni 5 minuti (un nome già esistente viene aggiornato, non duplicato)
select cron.schedule('bot-watchdog', '*/5 * * * *', $$select public.watchdog_check()$$);
