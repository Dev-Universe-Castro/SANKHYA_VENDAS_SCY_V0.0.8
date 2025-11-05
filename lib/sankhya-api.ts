import axios from 'axios';
import { getCacheService } from './redis-cache-wrapper';
import { apiLogger } from './api-logger';

// Configuração da API
const ENDPOINT_LOGIN = "https://api.sandbox.sankhya.com.br/login";
const URL_CONSULTA_SERVICO = "https://api.sandbox.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json";
const URL_SAVE_SERVICO = "https://api.sandbox.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.saveRecord&outputType=json";

// LOGIN_HEADERS removido - agora usa credenciais do contrato ativo

// Pool de conexões HTTP otimizado
const http = require('http');
const https = require('https');

const httpAgent = new http.Agent({
  keepAlive: true,
  keepAliveMsecs: 30000,
  maxSockets: 50,
  maxFreeSockets: 10,
  timeout: 30000
});

const httpsAgent = new https.Agent({
  keepAlive: true,
  keepAliveMsecs: 30000,
  maxSockets: 50,
  maxFreeSockets: 10,
  timeout: 30000,
  rejectUnauthorized: true
});

// Instância axios otimizada
const axiosInstance = axios.create({
  httpAgent,
  httpsAgent,
  timeout: 20000,
  maxContentLength: 50 * 1024 * 1024, // 50MB
  maxBodyLength: 50 * 1024 * 1024
});

// Token gerenciado APENAS via Redis (não usar variáveis locais)
let tokenPromise: Promise<string> | null = null;

// Type definitions for Redis token cache
interface TokenCache {
  token: string;
  expiresAt: number; // Timestamp in milliseconds
  geradoEm: string; // ISO string
}

interface TokenStatus {
  ativo: boolean;
  token: string | null;
  expiraEm: string;
  geradoEm: string;
  tempoRestanteMs: number;
  tempoRestanteMin: number;
}

/**
 * Obtém informações do token atual sem gerar um novo
 */
export async function obterTokenAtual(): Promise<TokenStatus | null> {
  try {
    console.log('🔍 [obterTokenAtual] Buscando token do Redis...');
    const cache = await getCacheService();
    const tokenData = await cache.get<TokenCache>('sankhya:token');

    if (!tokenData) {
      console.log('⚠️ [obterTokenAtual] Token não encontrado no Redis');
      return null;
    }

    console.log('📋 [obterTokenAtual] Token encontrado:', {
      hasToken: !!tokenData.token,
      geradoEm: tokenData.geradoEm,
      expiresAt: new Date(tokenData.expiresAt).toISOString()
    });

    const agora = Date.now();
    const tempoRestante = tokenData.expiresAt - agora;
    const ativo = tempoRestante > 0;

    const result = {
      ativo,
      token: ativo ? tokenData.token : null,
      expiraEm: new Date(tokenData.expiresAt).toISOString(),
      geradoEm: tokenData.geradoEm,
      tempoRestanteMs: Math.max(0, tempoRestante),
      tempoRestanteMin: Math.max(0, Math.floor(tempoRestante / 60000))
    };

    console.log('✅ [obterTokenAtual] Status do token:', {
      ativo: result.ativo,
      tempoRestanteMin: result.tempoRestanteMin,
      tokenPreview: result.token ? result.token.substring(0, 50) + '...' : null
    });

    return result;
  } catch (erro) {
    console.error('❌ [obterTokenAtual] Erro ao obter token atual:', erro);
    return null;
  }
}

const LOCK_KEY = 'sankhya:token:lock';
const TOKEN_CACHE_KEY = 'sankhya:token';

// Função para forçar renovação do token (exposta para o painel admin)
export async function obterToken(contratoId?: number, forceRefresh = false, retryCount = 0): Promise<string> {
  const cacheService = await getCacheService();

  // Buscar contrato ativo se não especificado
  if (!contratoId) {
    const contratoAtivo = await buscarContratoAtivo();
    if (!contratoAtivo) {
      throw new Error("Nenhum contrato ativo encontrado");
    }
    contratoId = contratoAtivo.ID_EMPRESA;
  }

  const TOKEN_KEY = `${TOKEN_CACHE_KEY}:${contratoId}`;

  // Se forçar refresh, limpar cache do Redis
  if (forceRefresh) {
    await cacheService.delete(TOKEN_KEY);
    console.log("🔄 Forçando renovação do token...");
  }

  // Verificar cache primeiro
  let tokenData = await cacheService.get<TokenCache>(TOKEN_KEY);
  if (tokenData && tokenData.token) {
    const agora = Date.now();
    const tempoRestante = tokenData.expiresAt - agora;
    if (tempoRestante > 0 && !forceRefresh) {
      console.log("✅ [obterToken] Token válido encontrado no Redis");
      return tokenData.token;
    }
  }

  // Se já está buscando token, aguardar
  if (tokenPromise) {
    console.log("⏳ [obterToken] Aguardando requisição de token em andamento...");
    return tokenPromise;
  }

  // Lock distribuído
  const LOCK_KEY_CONTRATO = `${LOCK_KEY}:${contratoId}`;
  const LOCK_TTL = 30000;
  const MAX_LOCK_WAIT = 25000;
  let lockAcquired = false;
  const lockStart = Date.now();

  while (!lockAcquired && (Date.now() - lockStart) < MAX_LOCK_WAIT) {
    try {
      const lockValue = `${Date.now()}-${Math.random()}`;
      const existing = await cacheService.get(LOCK_KEY_CONTRATO);

      if (!existing) {
        await cacheService.set(LOCK_KEY_CONTRATO, lockValue, LOCK_TTL);
        lockAcquired = true;
        console.log("🔒 [obterToken] Lock adquirido");
        break;
      }

      await new Promise(resolve => setTimeout(resolve, 500));

      tokenData = await cacheService.get<TokenCache>(TOKEN_KEY);
      if (tokenData && tokenData.token) {
        const agora = Date.now();
        const tempoRestante = tokenData.expiresAt - agora;
        if (tempoRestante > 0) {
          console.log("✅ [obterToken] Token gerado por outra requisição");
          await cacheService.delete(LOCK_KEY_CONTRATO).catch(() => {});
          return tokenData.token;
        }
      }
    } catch (error) {
      console.error("❌ [obterToken] Erro ao adquirir lock:", error);
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  }

  if (!lockAcquired) {
    throw new Error("Não foi possível gerar token - timeout ao aguardar lock");
  }

  // Buscar credenciais do contrato
  const contrato = await buscarContratoPorId(contratoId);
  if (!contrato) {
    await cacheService.delete(LOCK_KEY_CONTRATO).catch(() => {});
    throw new Error("Contrato não encontrado");
  }

  const MAX_RETRIES = 3;
  const RETRY_DELAY = 1000;

  tokenPromise = (async () => {
    try {
      console.log("🔐 Solicitando novo token de autenticação para contrato:", contratoId);
      
      const loginHeaders = {
        'token': contrato.SANKHYA_TOKEN,
        'appkey': contrato.SANKHYA_APPKEY,
        'username': contrato.SANKHYA_USERNAME,
        'password': contrato.SANKHYA_PASSWORD
      };

      const resposta = await axiosInstance.post(ENDPOINT_LOGIN, {}, {
        headers: loginHeaders,
        timeout: 10000
      });

      const token = resposta.data.bearerToken || resposta.data.token;

      if (!token) {
        throw new Error("Token não encontrado na resposta");
      }

      const geradoEm = new Date().toISOString();
      const expiresAt = Date.now() + (20 * 60 * 1000);

      const tokenData: TokenCache = {
        token,
        expiresAt,
        geradoEm
      };

      await cacheService.set(TOKEN_KEY, tokenData, 20 * 60);
      console.log("💾 [obterToken] Token salvo no Redis");

      await cacheService.delete(LOCK_KEY_CONTRATO).catch(() => {});
      console.log("🔓 [obterToken] Lock liberado");

      return token;

    } catch (erro: any) {
      await cacheService.delete(LOCK_KEY_CONTRATO).catch(() => {});

      if (erro.response?.status === 500 && retryCount < MAX_RETRIES) {
        console.log(`🔄 Tentando novamente (${retryCount + 1}/${MAX_RETRIES})...`);
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * (retryCount + 1)));
        tokenPromise = null;
        return obterToken(contratoId, forceRefresh, retryCount + 1);
      }

      await cacheService.delete(TOKEN_KEY).catch(() => {});
      tokenPromise = null;

      if (erro.response?.status === 500) {
        throw new Error("Serviço Sankhya temporariamente indisponível");
      }

      throw new Error(`Falha na autenticação: ${erro.response?.data?.error || erro.message}`);
    } finally {
      tokenPromise = null;
      await cacheService.delete(LOCK_KEY_CONTRATO).catch(() => {});
    }
  })();

  return tokenPromise;
}

// Função auxiliar para buscar contrato ativo
async function buscarContratoAtivo() {
  try {
    const { listarContratos } = await import('./oracle-service');
    const contratos = await listarContratos();
    return contratos.find((c: any) => c.ATIVO === true);
  } catch (error) {
    console.error("Erro ao buscar contrato ativo:", error);
    return null;
  }
}

// Função auxiliar para buscar contrato por ID
async function buscarContratoPorId(id: number) {
  try {
    const { buscarContratoPorId: buscarContrato } = await import('./oracle-service');
    return await buscarContrato(id);
  } catch (error) {
    console.error("Erro ao buscar contrato:", error);
    return null;
  }
}

// Função para invalidar o token no cache
export async function invalidarToken(): Promise<void> {
  try {
    const cacheService = await getCacheService();
    await cacheService.delete(TOKEN_CACHE_KEY);
    await cacheService.delete(LOCK_KEY); // Limpar também o lock se existir
    console.log('🗑️ Token invalidado do cache');
  } catch (error) {
    console.error('❌ Erro ao invalidar token:', error);
  }
}

// Requisição Autenticada Genérica
export async function fazerRequisicaoAutenticada(fullUrl: string, method = 'POST', data = {}, retryCount = 0) {
  const MAX_RETRIES = 2;
  const RETRY_DELAY = 1000;
  const startTime = Date.now();

  try {
    const token = await obterToken();
    const cacheService = await getCacheService(); // Obter cache service para logar

    const config = {
      method: method.toLowerCase(),
      url: fullUrl,
      data: data,
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json'
      },
      timeout: 15000
    };

    const response = await axiosInstance(config); // Renamed from 'resposta' to 'response' for clarity

    // Adicionar log de sucesso
    const duration = Date.now() - startTime;
    try {
      const addApiLog = (await import('@/app/api/admin/api-logs/route')).addApiLog;
      if (addApiLog) {
        // Tentar obter informações do usuário dos cookies
        let userId, userName;
        try {
          const { cookies } = await import('next/headers');
          const cookieStore = cookies();
          const userCookie = cookieStore.get('user');
          if (userCookie) {
            const userData = JSON.parse(userCookie.value);
            userId = userData.id;
            userName = userData.nome || userData.email;
          }
        } catch (e) {
          // Ignorar se não conseguir obter cookies
        }

        addApiLog({
          method: method.toUpperCase(),
          url: fullUrl,
          status: response.status,
          duration,
          tokenUsed: true,
          userId,
          userName
        });
      }
    } catch (e) {
      // Ignorar se módulo não disponível
    }

    return response.data;

  } catch (erro: any) {
    // Adicionar log de erro
    const duration = Date.now() - startTime;
    const errorStatus = erro.response?.status || 500;
    const errorMessage = erro.response?.data?.statusMessage || erro.message || 'Erro desconhecido';

    try {
      const addApiLog = (await import('@/app/api/admin/api-logs/route')).addApiLog;
      if (addApiLog) {
        // Tentar obter informações do usuário dos cookies
        let userId, userName;
        try {
          const { cookies } = await import('next/headers');
          const cookieStore = cookies();
          const userCookie = cookieStore.get('user');
          if (userCookie) {
            const userData = JSON.parse(userCookie.value);
            userId = userData.id;
            userName = userData.nome || userData.email;
          }
        } catch (e) {
          // Ignorar se não conseguir obter cookies
        }

        addApiLog({
          method: method.toUpperCase(),
          url: fullUrl,
          status: errorStatus,
          duration,
          tokenUsed: !!erro.response,
          error: errorMessage,
          userId,
          userName
        });
      }
    } catch (e) {
      // Ignorar se módulo não disponível
      console.warn("Módulo de logs da API não disponível:", e);
    }

    // Se token expirou, limpar cache do Redis e tentar novamente
    if (erro.response && (erro.response.status === 401 || erro.response.status === 403)) {
      await getCacheService().then(cache => cache.delete('sankhya:token')).catch(() => {});

      if (retryCount < 1) {
        console.log("🔄 Token expirado, obtendo novo token...");
        await new Promise(resolve => setTimeout(resolve, 500));
        return fazerRequisicaoAutenticada(fullUrl, method, data, retryCount + 1);
      }

      throw new Error("Sessão expirada. Tente novamente.");
    }

    // Retry para erros de rede ou timeout
    if ((erro.code === 'ECONNABORTED' || erro.code === 'ENOTFOUND' || erro.response?.status >= 500) && retryCount < MAX_RETRIES) {
      console.log(`🔄 Tentando novamente requisição (${retryCount + 1}/${MAX_RETRIES})...`);
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * (retryCount + 1)));
      return fazerRequisicaoAutenticada(fullUrl, method, data, retryCount + 1);
    }

    const errorDetails = erro.response?.data || erro.message;
    console.error("❌ Erro na requisição Sankhya:", {
      url: fullUrl,
      method,
      error: errorDetails
    });

    // Mensagem de erro mais amigável
    if (erro.code === 'ECONNABORTED') {
      throw new Error("Tempo de resposta excedido. Tente novamente.");
    }

    if (erro.response?.status >= 500) {
      throw new Error("Serviço temporariamente indisponível. Tente novamente.");
    }

    throw new Error(erro.response?.data?.statusMessage || erro.message || "Erro na comunicação com o servidor");
  }
}

// Mapeamento de Parceiros
function mapearParceiros(entities: any) {
  const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);

  // Se entity não é um array, converte para array
  const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

  return entityArray.map((rawEntity: any, index: number) => {
    const cleanObject: any = {};

    for (let i = 0; i < fieldNames.length; i++) {
      const fieldKey = `f${i}`;
      const fieldName = fieldNames[i];

      if (rawEntity[fieldKey]) {
        cleanObject[fieldName] = rawEntity[fieldKey].$;
      }
    }

    cleanObject._id = cleanObject.CODPARC ? String(cleanObject.CODPARC) : String(index);
    return cleanObject;
  });
}

// Consultar Parceiros com Paginação
export async function consultarParceiros(page: number = 1, pageSize: number = 50, searchName: string = '', searchCode: string = '', codVendedor?: number, codVendedoresEquipe?: number[]) {
  // Criar chave de cache baseada nos parâmetros
  const cacheKey = `parceiros:list:${page}:${pageSize}:${searchName}:${searchCode}:${codVendedor}:${codVendedoresEquipe?.join(',')}`;
  const cacheService = await getCacheService();
  const cached = await cacheService.get<any>(cacheKey);

  if (cached !== null) {
    console.log('✅ Retornando parceiros do cache');
    return cached;
  }

  // Construir critério de busca
  const filters: string[] = [];

  // SEMPRE filtrar apenas CLIENTES (CLIENTE = 'S')
  filters.push(`CLIENTE = 'S'`);

  // Filtro por código do parceiro
  if (searchCode.trim() !== '') {
    const code = searchCode.trim();
    filters.push(`CODPARC = ${code}`);
  }

  // Filtro por nome do parceiro
  if (searchName.trim() !== '') {
    const name = searchName.trim().toUpperCase();
    filters.push(`NOMEPARC LIKE '%${name}%'`);
  }

  // Filtro por vendedor ou equipe do gerente
  if (codVendedoresEquipe && codVendedoresEquipe.length > 0) {
    // Se é gerente com equipe, buscar clientes APENAS dos vendedores da equipe
    const vendedoresList = codVendedoresEquipe.join(',');
    console.log('🔍 Aplicando filtro de equipe do gerente:', vendedoresList);
    filters.push(`CODVEND IN (${vendedoresList})`);
    // Garantir que CODVEND não seja nulo
    filters.push(`CODVEND IS NOT NULL`);
  } else if (codVendedor) {
    // Se é vendedor, buscar APENAS clientes com esse vendedor preferencial
    console.log('🔍 Aplicando filtro de vendedor único:', codVendedor);
    filters.push(`CODVEND = ${codVendedor}`);
    filters.push(`CODVEND IS NOT NULL`);
  } else {
    console.log('⚠️ Nenhum filtro de vendedor aplicado - buscando todos');
  }

  // Junta todos os filtros com AND
  const criteriaExpression = filters.join(' AND ');

  // Monta o payload base
  const dataSet: any = {
    "rootEntity": "Parceiro",
    "includePresentationFields": "N",
    "offsetPage": null,
    "disableRowsLimit": true,
    "entity": {
      "fieldset": {
        "list": "CODPARC, NOMEPARC, CGC_CPF, CODCID, ATIVO, TIPPESSOA, RAZAOSOCIAL, IDENTINSCESTAD, CEP, CODEND, NUMEND, COMPLEMENTO, CODBAI, LATITUDE, LONGITUDE, CLIENTE, CODVEND"
      }
    },
    "criteria": {
      "expression": {
        "$": criteriaExpression
      }
    }
  };

  const PARCEIROS_PAYLOAD = {
    "requestBody": {
      "dataSet": dataSet
    }
  };

  try {
    console.log("🔍 Buscando parceiros com filtro:", {
      page,
      pageSize,
      searchName,
      searchCode,
      criteriaExpression
    });

    const respostaCompleta = await fazerRequisicaoAutenticada(
      URL_CONSULTA_SERVICO,
      'POST',
      PARCEIROS_PAYLOAD
    );

    console.log("📦 Resposta da consulta recebida:", {
      hasResponseBody: !!respostaCompleta.responseBody,
      hasEntities: !!respostaCompleta.responseBody?.entities,
      total: respostaCompleta.responseBody?.entities?.total
    });

    // Verificar se a resposta tem a estrutura esperada
    if (!respostaCompleta.responseBody || !respostaCompleta.responseBody.entities) {
      console.log("⚠️ Resposta da API sem estrutura esperada:", {
        status: respostaCompleta.status,
        serviceName: respostaCompleta.serviceName
      });

      return {
        parceiros: [],
        total: 0,
        page,
        pageSize,
        totalPages: 0
      };
    }

    const entities = respostaCompleta.responseBody.entities;

    // Se não houver resultados, retorna array vazio
    if (!entities || !entities.entity) {
      console.log("ℹ️ Nenhum parceiro encontrado:", {
        total: entities?.total || 0,
        hasMoreResult: entities?.hasMoreResult,
        criteriaExpression
      });

      return {
        parceiros: [],
        total: 0,
        page,
        pageSize,
        totalPages: 0
      };
    }

    const listaParceirosLimpa = mapearParceiros(entities);
    const total = entities.total ? parseInt(entities.total) : listaParceirosLimpa.length;

    // Retornar dados paginados com informações adicionais
    const resultado = {
      parceiros: listaParceirosLimpa,
      total,
      page,
      pageSize,
      totalPages: Math.ceil(total / pageSize)
    };

    // Salvar no cache (TTL automático para parceiros: 10 minutos)
    await cacheService.set(cacheKey, resultado, 10 * 60); // 10 minutos em segundos

    return resultado;

  } catch (erro) {
    throw erro;
  }
}

// Consultar Tipos de Operação
export async function consultarTiposOperacao() {
  const cacheKey = 'tipos:operacao:all';
  const cacheService = await getCacheService();
  const cached = await cacheService.get<any>(cacheKey);

  if (cached !== null) {
    console.log('✅ Retornando tipos de operação do cache');
    return cached;
  }

  const PAYLOAD = {
    "requestBody": {
      "dataSet": {
        "rootEntity": "TipoOperacao",
        "includePresentationFields": "N",
        "offsetPage": null,
        "disableRowsLimit": true,     
        "entity": {
          "fieldset": {
            "list": "CODTIPOPER, DESCROPER, ATIVO"
          }
        },
        "criteria": {
          "expression": {
            "$": "ATIVO = 'S'"
          }
        },
        "orderBy": {
          "expression": {
            "$": "DESCROPER ASC"
          }
        }
      }
    }
  };

  try {
    console.log("🔍 Buscando tipos de operação...");

    const respostaCompleta = await fazerRequisicaoAutenticada(
      URL_CONSULTA_SERVICO,
      'POST',
      PAYLOAD
    );

    const entities = respostaCompleta.responseBody.entities;

    if (!entities || !entities.entity) {
      console.log("ℹ️ Nenhum tipo de operação encontrado");
      return [];
    }

    const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
    const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

    const tiposOperacao = entityArray.map((rawEntity: any) => {
      const cleanObject: any = {};
      for (let i = 0; i < fieldNames.length; i++) {
        const fieldKey = `f${i}`;
        const fieldName = fieldNames[i];
        if (rawEntity[fieldKey]) {
          cleanObject[fieldName] = rawEntity[fieldKey].$;
        }
      }
      return cleanObject;
    });

    console.log(`✅ ${tiposOperacao.length} tipos de operação encontrados`);

    // Salvar no cache (60 minutos - raramente muda)
    await cacheService.set(cacheKey, tiposOperacao, 60 * 60); // 60 minutos em segundos

    return tiposOperacao;

  } catch (erro) {
    console.error("❌ Erro ao consultar tipos de operação:", erro);
    throw erro;
  }
}

// Consultar Tipos de Negociação
export async function consultarTiposNegociacao() {
  const cacheKey = 'tipos:negociacao:all';
  const cacheService = await getCacheService();
  const cached = await cacheService.get<any>(cacheKey);

  if (cached !== null) {
    console.log('✅ Retornando tipos de negociação do cache');
    return cached;
  }

  const PAYLOAD = {
    "requestBody": {
      "dataSet": {
        "rootEntity": "TipoNegociacao",
        "includePresentationFields": "N",
        "offsetPage": null,
        "disableRowsLimit": true,
        "entity": {
          "fieldset": {
            "list": "CODTIPVENDA, DESCRTIPVENDA"
          }
        },
        "criteria": {
          "expression": {
            "$": "ATIVO = 'S'"
          }
        },
        "orderBy": {
          "expression": {
            "$": "DESCRTIPVENDA ASC"
          }
        }
      }
    }
  };

  try {
    console.log("🔍 Buscando tipos de negociação...");

    const respostaCompleta = await fazerRequisicaoAutenticada(
      URL_CONSULTA_SERVICO,
      'POST',
      PAYLOAD
    );

    const entities = respostaCompleta.responseBody.entities;

    if (!entities || !entities.entity) {
      console.log("ℹ️ Nenhum tipo de negociação encontrado");
      return [];
    }

    const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
    const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

    const tiposNegociacao = entityArray.map((rawEntity: any) => {
      const cleanObject: any = {};
      for (let i = 0; i < fieldNames.length; i++) {
        const fieldKey = `f${i}`;
        const fieldName = fieldNames[i];
        if (rawEntity[fieldKey]) {
          cleanObject[fieldName] = rawEntity[fieldKey].$;
        }
      }
      return cleanObject;
    });

    console.log(`✅ ${tiposNegociacao.length} tipos de negociação encontrados`);

    // Salvar no cache (60 minutos)
    await cacheService.set(cacheKey, tiposNegociacao, 60 * 60); // 60 minutos em segundos

    return tiposNegociacao;

  } catch (erro) {
    console.error("❌ Erro ao consultar tipos de negociação:", erro);
    throw erro;
  }
}

// Consultar Complemento do Parceiro
export async function consultarComplementoParceiro(codParc: string) {
  const cacheKey = `parceiros:complemento:${codParc}`;
  const cacheService = await getCacheService();
  const cached = await cacheService.get<any>(cacheKey);

  if (cached !== null) {
    console.log(`✅ Retornando complemento do parceiro ${codParc} do cache`);
    return cached;
  }

  const PAYLOAD = {
    "requestBody": {
      "dataSet": {
        "rootEntity": "ComplementoParc",
        "includePresentationFields": "N",
        "offsetPage": "0",
        "limit": "1",
        "entity": {
          "fieldset": {
            "list": "CODPARC, SUGTIPNEGSAID"
          }
        },
        "criteria": {
          "expression": {
            "$": `CODPARC = ${codParc}`
          }
        }
      }
    }
  };

  try {
    console.log(`🔍 Buscando complemento do parceiro ${codParc}...`);

    const respostaCompleta = await fazerRequisicaoAutenticada(
      URL_CONSULTA_SERVICO,
      'POST',
      PAYLOAD
    );

    const entities = respostaCompleta.responseBody.entities;

    if (!entities || !entities.entity) {
      console.log("ℹ️ Nenhum complemento encontrado para o parceiro");
      return null;
    }

    const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
    const rawEntity = Array.isArray(entities.entity) ? entities.entity[0] : entities.entity;

    const complemento: any = {};
    for (let i = 0; i < fieldNames.length; i++) {
      const fieldKey = `f${i}`;
      const fieldName = fieldNames[i];
      if (rawEntity[fieldKey]) {
        complemento[fieldName] = rawEntity[fieldKey].$;
      }
    }

    console.log(`✅ Complemento encontrado:`, complemento);

    // Salvar no cache (10 minutos)
    await cacheService.set(cacheKey, complemento, 10 * 60); // 10 minutos em segundos

    return complemento;

  } catch (erro) {
    console.error("❌ Erro ao consultar complemento do parceiro:", erro);
    return null;
  }
}

// Criar/Atualizar Parceiro
export async function salvarParceiro(parceiro: {
  CODPARC?: string;
  NOMEPARC: string;
  CGC_CPF: string;
  CODCID: string;
  ATIVO: string;
  TIPPESSOA: string;
  CODVEND?: number;
  RAZAOSOCIAL?: string;
  IDENTINSCESTAD?: string;
  CEP?: string;
  CODEND?: string;
  NUMEND?: string;
  COMPLEMENTO?: string;
  CODBAI?: string;
  LATITUDE?: string;
  LONGITUDE?: string;
}) {
  const cacheService = await getCacheService(); // Obter cache service para invalidar cache

  // Se tem CODPARC, é atualização (usa DatasetSP.save com pk)
  if (parceiro.CODPARC) {
    const URL_UPDATE_SERVICO = "https://api.sandbox.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=DatasetSP.save&outputType=json";

    const UPDATE_PAYLOAD = {
      "serviceName": "DatasetSP.save",
      "requestBody": {
        "entityName": "Parceiro",
        "standAlone": false,
        "fields": [
          "CODPARC",
          "NOMEPARC",
          "ATIVO",
          "TIPPESSOA",
          "CGC_CPF",
          "CODCID",
          "CODVEND",
          "RAZAOSOCIAL",
          "IDENTINSCESTAD",
          "CEP",
          "CODEND",
          "NUMEND",
          "COMPLEMENTO",
          "CODBAI",
          "LATITUDE",
          "LONGITUDE"
        ],
        "records": [
          {
            "pk": {
              "CODPARC": String(parceiro.CODPARC)
            },
            "values": {
              "1": parceiro.NOMEPARC,
              "2": parceiro.ATIVO,
              "3": parceiro.TIPPESSOA,
              "4": parceiro.CGC_CPF,
              "5": parceiro.CODCID,
              "6": parceiro.CODVEND || null,
              "7": parceiro.RAZAOSOCIAL || "",
              "8": parceiro.IDENTINSCESTAD || "",
              "9": parceiro.CEP || "",
              "10": parceiro.CODEND || "",
              "11": parceiro.NUMEND || "",
              "12": parceiro.COMPLEMENTO || "",
              "13": parceiro.CODBAI || "",
              "14": parceiro.LATITUDE || "",
              "15": parceiro.LONGITUDE || ""
            }
          }
        ]
      }
    };

    try {
      console.log("📤 Enviando requisição para atualizar parceiro:", {
        codigo: parceiro.CODPARC,
        nome: parceiro.NOMEPARC,
        cpfCnpj: parceiro.CGC_CPF,
        cidade: parceiro.CODCID,
        ativo: parceiro.ATIVO,
        tipo: parceiro.TIPPESSOA
      });

      const resposta = await fazerRequisicaoAutenticada(
        URL_UPDATE_SERVICO,
        'POST',
        UPDATE_PAYLOAD
      );

      console.log("✅ Parceiro atualizado com sucesso:", resposta);

      // Invalidar cache de parceiros
      await cacheService.invalidateParceiros();
      console.log('🗑️ Cache de parceiros invalidado');

      return resposta;
    } catch (erro: any) {
      console.error("❌ Erro ao atualizar Parceiro Sankhya:", {
        message: erro.message,
        codigo: parceiro.CODPARC,
        dados: {
          nome: parceiro.NOMEPARC,
          cpfCnpj: parceiro.CGC_CPF,
          cidade: parceiro.CODCID
        }
      });
      throw erro;
    }
  }

  // Se não tem CODPARC, é criação (usa DatasetSP.save)
  const URL_CREATE_SERVICO = "https://api.sandbox.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=DatasetSP.save&outputType=json";

  const CREATE_PAYLOAD = {
    "serviceName": "DatasetSP.save",
    "requestBody": {
      "entityName": "Parceiro",
      "standAlone": false,
      "fields": [
        "CODPARC",
        "NOMEPARC",
        "ATIVO",
        "TIPPESSOA",
        "CGC_CPF",
        "CODCID",
        "CODVEND",
        "RAZAOSOCIAL",
        "IDENTINSCESTAD",
        "CEP",
        "CODEND",
        "NUMEND",
        "COMPLEMENTO",
        "CODBAI",
        "LATITUDE",
        "LONGITUDE"
      ],
      "records": [
        {
          "values": {
            "1": parceiro.NOMEPARC,
            "2": parceiro.ATIVO,
            "3": parceiro.TIPPESSOA,
            "4": parceiro.CGC_CPF,
            "5": parceiro.CODCID,
            "6": parceiro.CODVEND || null,
            "7": parceiro.RAZAOSOCIAL || "",
            "8": parceiro.IDENTINSCESTAD || "",
            "9": parceiro.CEP || "",
            "10": parceiro.CODEND || "",
            "11": parceiro.NUMEND || "",
            "12": parceiro.COMPLEMENTO || "",
            "13": parceiro.CODBAI || "",
            "14": parceiro.LATITUDE || "",
            "15": parceiro.LONGITUDE || ""
          }
        }
      ]
    }
  };

  try {
    console.log("📤 Enviando requisição para criar parceiro:", {
      nome: parceiro.NOMEPARC,
      cpfCnpj: parceiro.CGC_CPF,
      cidade: parceiro.CODCID,
      ativo: parceiro.ATIVO,
      tipo: parceiro.TIPPESSOA
    });

    const resposta = await fazerRequisicaoAutenticada(
      URL_CREATE_SERVICO,
      'POST',
      CREATE_PAYLOAD
    );

    console.log("✅ Parceiro criado com sucesso:", resposta);

    // Invalidar cache de parceiros
    await cacheService.invalidateParceiros();
    console.log('🗑️ Cache de parceiros invalidado');

    return resposta;
  } catch (erro: any) {
    console.error("❌ Erro ao criar Parceiro Sankhya:", {
      message: erro.message,
      dados: {
        nome: parceiro.NOMEPARC,
        cpfCnpj: parceiro.CGC_CPF,
        cidade: parceiro.CODCID
      }
    });
    throw erro;
  }
}


// Consultar CODTIPVENDA e NUNOTA do CabecalhoNota por CODTIPOPER
export async function consultarTipVendaPorModelo(codTipOper: string) {
  const PAYLOAD = {
    "requestBody": {
      "dataSet": {
        "rootEntity": "CabecalhoNota",
        "includePresentationFields": "N",
        "offsetPage": "0",
        "limit": "1",
        "entity": {
          "fieldset": {
            "list": "NUNOTA, CODTIPOPER, CODTIPVENDA"
          }
        },
        "criteria": {
          "expression": {
            "$": `TIPMOV = 'Z' AND CODTIPOPER = ${codTipOper}`
          }
        },
        "orderBy": {
          "expression": {
            "$": "NUNOTA DESC"
          }
        }
      }
    }
  };

  try {
    console.log(`🔍 Buscando CODTIPVENDA e NUNOTA para modelo ${codTipOper} com TIPMOV = 'Z'...`);

    const respostaCompleta = await fazerRequisicaoAutenticada(
      URL_CONSULTA_SERVICO,
      'POST',
      PAYLOAD
    );

    const entities = respostaCompleta.responseBody.entities;

    if (!entities || !entities.entity) {
      console.log("ℹ️ Nenhum CabecalhoNota encontrado para este modelo");
      return { codTipVenda: null, nunota: null };
    }

    const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
    const rawEntity = Array.isArray(entities.entity) ? entities.entity[0] : entities.entity;

    const cabecalho: any = {};
    for (let i = 0; i < fieldNames.length; i++) {
      const fieldKey = `f${i}`;
      const fieldName = fieldNames[i];
      if (rawEntity[fieldKey]) {
        cabecalho[fieldName] = rawEntity[fieldKey].$;
      }
    }

    console.log(`✅ CODTIPVENDA e NUNOTA encontrados:`, { codTipVenda: cabecalho.CODTIPVENDA, nunota: cabecalho.NUNOTA });
    return { codTipVenda: cabecalho.CODTIPVENDA, nunota: cabecalho.NUNOTA };

  } catch (erro) {
    console.error("❌ Erro ao consultar CODTIPVENDA e NUNOTA do CabecalhoNota:", erro);
    return { codTipVenda: null, nunota: null };
  }
}

// Consultar dados completos do modelo da nota por NUNOTA
export async function consultarDadosModeloNota(nunota: string) {
  const PAYLOAD = {
    "requestBody": {
      "dataSet": {
        "rootEntity": "CabecalhoNota",
        "includePresentationFields": "N",
        "offsetPage": "0",
        "limit": "1",
        "entity": {
          "fieldset": {
            "list": "NUNOTA, CODTIPOPER, CODTIPVENDA"
          }
        },
        "criteria": {
          "expression": {
            "$": `NUNOTA = ${nunota}`
          }
        }
      }
    }
  };

  try {
    console.log(`🔍 Buscando dados do modelo NUNOTA ${nunota}...`);

    const respostaCompleta = await fazerRequisicaoAutenticada(
      URL_CONSULTA_SERVICO,
      'POST',
      PAYLOAD
    );

    const entities = respostaCompleta.responseBody.entities;

    if (!entities || !entities.entity) {
      console.log("ℹ️ Nenhum modelo encontrado para este NUNOTA");
      return { codTipOper: null, codTipVenda: null };
    }

    const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
    const rawEntity = Array.isArray(entities.entity) ? entities.entity[0] : entities.entity;

    const cabecalho: any = {};
    for (let i = 0; i < fieldNames.length; i++) {
      const fieldKey = `f${i}`;
      const fieldName = fieldNames[i];
      if (rawEntity[fieldKey]) {
        cabecalho[fieldName] = rawEntity[fieldKey].$;
      }
    }

    console.log(`✅ Dados do modelo encontrados:`, {
      codTipOper: cabecalho.CODTIPOPER,
      codTipVenda: cabecalho.CODTIPVENDA
    });

    return {
      codTipOper: cabecalho.CODTIPOPER,
      codTipVenda: cabecalho.CODTIPVENDA
    };

  } catch (erro) {
    console.error("❌ Erro ao consultar dados do modelo da nota:", erro);
    return { codTipOper: null, codTipVenda: null };
  }
}