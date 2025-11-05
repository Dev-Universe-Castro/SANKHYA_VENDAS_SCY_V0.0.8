/**
 * Este arquivo é executado automaticamente pelo Next.js quando o servidor inicia
 * Ele roda apenas UMA VEZ, antes de qualquer requisição
 * 
 * Documentação: https://nextjs.org/docs/app/building-your-application/optimizing/instrumentation
 */

export async function register() {
  if (process.env.NEXT_RUNTIME === 'nodejs') {
    console.log('[INSTRUMENTATION] Registering server-side instrumentation...');

    // Carregar variáveis de ambiente
    if (typeof window === 'undefined') {
      const dotenv = require('dotenv');
      const path = require('path');

      const result = dotenv.config({ path: path.resolve(process.cwd(), 'config.env.local') });

      if (result.parsed) {
        console.log(`✅ Variáveis carregadas do config.env.local: ${Object.keys(result.parsed).length}`);
      }
    }
  } else {
    console.log('[INSTRUMENTATION] Não está rodando no runtime nodejs, pulando inicialização');
  }
}