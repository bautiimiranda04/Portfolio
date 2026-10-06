-- Disparo puntual de los workflows de GitHub desde Supabase (pg_cron + pg_net).
-- Se corre una vez en Supabase → SQL Editor. Es idempotente: se puede volver a correr.
-- Horarios en UTC (Argentina = UTC-3, sin horario de verano).
--
-- La llave (fine-grained PAT: solo este repo, Actions: Read and write) va en
-- Supabase Vault. NUNCA pegarla en este archivo: el repo es público.

create extension if not exists pg_cron;
create extension if not exists pg_net;

-- 1) Guardar / reemplazar la llave en Vault
do $$
declare v_id uuid;
begin
  select id into v_id from vault.secrets where name = 'github_pat';
  if v_id is null then
    perform vault.create_secret('PEGAR_LLAVE_ACA', 'github_pat');
  else
    perform vault.update_secret(v_id, 'PEGAR_LLAVE_ACA');
  end if;
end $$;

-- 2) Función que le pide a GitHub correr un workflow (schema privado: no expuesta por la API)
create schema if not exists private;
revoke all on schema private from public, anon, authenticated;

create or replace function private.trigger_github_workflow(workflow text)
returns bigint
language sql
security definer
set search_path = ''
as $$
  select net.http_post(
    url := 'https://api.github.com/repos/bautiimiranda04/Portfolio/actions/workflows/' || workflow || '/dispatches',
    headers := jsonb_build_object(
      'Authorization', 'Bearer ' || (select decrypted_secret from vault.decrypted_secrets where name = 'github_pat'),
      'Accept', 'application/vnd.github+json',
      'User-Agent', 'supabase-pg-cron',
      'Content-Type', 'application/json'
    ),
    body := '{"ref":"main"}'::jsonb
  );
$$;
revoke execute on function private.trigger_github_workflow(text) from public, anon, authenticated;

-- 3) Horarios
select cron.schedule('precios-10hs-arg',  '0 13 * * *',   $$select private.trigger_github_workflow('update-prices.yml')$$);
select cron.schedule('precios-18hs-arg',  '0 21 * * *',   $$select private.trigger_github_workflow('update-prices.yml')$$);
select cron.schedule('analyst-10hs-arg',  '5 13 * * 1-5', $$select private.trigger_github_workflow('analyze.yml')$$);

-- 4) Prueba inmediata: dispara una actualización ahora
select private.trigger_github_workflow('update-prices.yml');
