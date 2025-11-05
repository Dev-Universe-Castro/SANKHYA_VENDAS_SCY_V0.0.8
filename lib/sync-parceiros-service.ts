
import oracledb from 'oracledb';
import { getOracleConnection } from './oracle-service';
import { fazerRequisicaoAutenticada, obterToken } from './sankhya-api';

const URL_CONSULTA_SERVICO = "https://api.sandbox.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json";

interface ParceiroSankhya {
  CODPARC: number;
  NOMEPARC: string;
  CGC_CPF?: string;
  CODCID?: number;
  ATIVO?: string;
  TIPPESSOA?: string;
  RAZAOSOCIAL?: string;
  IDENTINSCESTAD?: string;
  CEP?: string;
  CODEND?: number;
  NUMEND?: string;
  COMPLEMENTO?: string;
  CODBAI?: number;
  LATITUDE?: string;
  LONGITUDE?: string;
  CLIENTE?: string;
  CODVEND?: number;
}

interface SyncResult {
  success: boolean;
  idSistema: number;
  empresa: string;
  totalRegistros: number;
  registrosInseridos: number;
  registrosAtualizados: number;
  registrosDeletados: number;
  dataInicio: Date;
  dataFim: Date;
  duracao: number;
  erro?: string;
}

/**
 * Busca todos os parceiros do Sankhya para uma empresa específica
 */
async function buscarParceirosSankhya(idSistema: number): Promise<ParceiroSankhya[]> {
  const PAYLOAD = {
    "requestBody": {
      "dataSet": {
        "offsetPage": null,
        "disableRowsLimit": true,
        "rootEntity": "Parceiro",
        "includePresentationFields": "N",
        "entity": {
          "fieldset": {
            "list": "CODPARC, NOMEPARC, CGC_CPF, CODCID, ATIVO, TIPPESSOA, RAZAOSOCIAL, IDENTINSCESTAD, CEP, CODEND, NUMEND, COMPLEMENTO, CODBAI, LATITUDE, LONGITUDE, CLIENTE, CODVEND"
          }
        }
      }
    }
  };

  console.log(`🔍 [Sync] Buscando parceiros do Sankhya para ID_SISTEMA: ${idSistema}`);

  const respostaCompleta = await fazerRequisicaoAutenticada(
    URL_CONSULTA_SERVICO,
    'POST',
    PAYLOAD
  );

  const entities = respostaCompleta.responseBody?.entities;

  if (!entities || !entities.entity) {
    console.log(`⚠️ [Sync] Nenhum parceiro encontrado para ID_SISTEMA: ${idSistema}`);
    return [];
  }

  const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
  const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

  const parceiros = entityArray.map((rawEntity: any) => {
    const cleanObject: any = {};
    for (let i = 0; i < fieldNames.length; i++) {
      const fieldKey = `f${i}`;
      const fieldName = fieldNames[i];
      if (rawEntity[fieldKey]) {
        cleanObject[fieldName] = rawEntity[fieldKey].$;
      }
    }
    return cleanObject as ParceiroSankhya;
  });

  console.log(`✅ [Sync] ${parceiros.length} parceiros recuperados do Sankhya`);
  return parceiros;
}

/**
 * Executa o soft delete (marca como não atual) todos os parceiros do sistema
 */
async function marcarTodosComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
  const result = await connection.execute(
    `UPDATE AS_PARCEIROS 
     SET SANKHYA_ATUAL = 'N', 
         DT_ULT_CARGA = CURRENT_TIMESTAMP 
     WHERE ID_SISTEMA = :idSistema 
       AND SANKHYA_ATUAL = 'S'`,
    { idSistema },
    { autoCommit: false }
  );

  const rowsAffected = result.rowsAffected || 0;
  console.log(`🗑️ [Sync] ${rowsAffected} registros marcados como não atuais`);
  return rowsAffected;
}

/**
 * Executa UPSERT de parceiros usando MERGE
 */
async function upsertParceiros(
  connection: oracledb.Connection, 
  idSistema: number, 
  parceiros: ParceiroSankhya[]
): Promise<{ inseridos: number; atualizados: number }> {
  let inseridos = 0;
  let atualizados = 0;

  // Processar em lotes de 100 para evitar sobrecarga
  const BATCH_SIZE = 100;
  
  for (let i = 0; i < parceiros.length; i += BATCH_SIZE) {
    const batch = parceiros.slice(i, i + BATCH_SIZE);
    
    for (const parceiro of batch) {
      const result = await connection.execute(
        `MERGE INTO AS_PARCEIROS dest
         USING (SELECT :idSistema AS ID_SISTEMA, :codParc AS CODPARC FROM DUAL) src
         ON (dest.ID_SISTEMA = src.ID_SISTEMA AND dest.CODPARC = src.CODPARC)
         WHEN MATCHED THEN
           UPDATE SET
             NOMEPARC = :nomeparc,
             CGC_CPF = :cgfCpf,
             CODCID = :codCid,
             ATIVO = :ativo,
             TIPPESSOA = :tipPessoa,
             RAZAOSOCIAL = :razaoSocial,
             IDENTINSCESTAD = :identInscEstad,
             CEP = :cep,
             CODEND = :codEnd,
             NUMEND = :numEnd,
             COMPLEMENTO = :complemento,
             CODBAI = :codBai,
             LATITUDE = :latitude,
             LONGITUDE = :longitude,
             CLIENTE = :cliente,
             CODVEND = :codVend,
             SANKHYA_ATUAL = 'S',
             DT_ULT_CARGA = CURRENT_TIMESTAMP
         WHEN NOT MATCHED THEN
           INSERT (
             ID_SISTEMA, CODPARC, NOMEPARC, CGC_CPF, CODCID, ATIVO, TIPPESSOA,
             RAZAOSOCIAL, IDENTINSCESTAD, CEP, CODEND, NUMEND, COMPLEMENTO,
             CODBAI, LATITUDE, LONGITUDE, CLIENTE, CODVEND, SANKHYA_ATUAL,
             DT_ULT_CARGA, DT_CRIACAO
           )
           VALUES (
             :idSistema, :codParc, :nomeparc, :cgfCpf, :codCid, :ativo, :tipPessoa,
             :razaoSocial, :identInscEstad, :cep, :codEnd, :numEnd, :complemento,
             :codBai, :latitude, :longitude, :cliente, :codVend, 'S',
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
           )`,
        {
          idSistema,
          codParc: parceiro.CODPARC,
          nomeparc: parceiro.NOMEPARC || null,
          cgfCpf: parceiro.CGC_CPF || null,
          codCid: parceiro.CODCID || null,
          ativo: parceiro.ATIVO || null,
          tipPessoa: parceiro.TIPPESSOA || null,
          razaoSocial: parceiro.RAZAOSOCIAL || null,
          identInscEstad: parceiro.IDENTINSCESTAD || null,
          cep: parceiro.CEP || null,
          codEnd: parceiro.CODEND || null,
          numEnd: parceiro.NUMEND || null,
          complemento: parceiro.COMPLEMENTO || null,
          codBai: parceiro.CODBAI || null,
          latitude: parceiro.LATITUDE || null,
          longitude: parceiro.LONGITUDE || null,
          cliente: parceiro.CLIENTE || null,
          codVend: parceiro.CODVEND || null
        },
        { autoCommit: false }
      );

      // Oracle não retorna se foi INSERT ou UPDATE no MERGE, então estimamos
      // Se rowsAffected > 0, foi uma operação bem-sucedida
      if (result.rowsAffected && result.rowsAffected > 0) {
        // Verificar se era novo ou atualização
        const checkResult = await connection.execute(
          `SELECT DT_CRIACAO FROM AS_PARCEIROS 
           WHERE ID_SISTEMA = :idSistema AND CODPARC = :codParc`,
          { idSistema, codParc: parceiro.CODPARC },
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );
        
        if (checkResult.rows && checkResult.rows.length > 0) {
          const row: any = checkResult.rows[0];
          const dtCriacao = new Date(row.DT_CRIACAO);
          const agora = new Date();
          const diferencaMs = agora.getTime() - dtCriacao.getTime();
          
          // Se foi criado há menos de 5 segundos, consideramos como inserção
          if (diferencaMs < 5000) {
            inseridos++;
          } else {
            atualizados++;
          }
        }
      }
    }
    
    // Commit a cada lote
    await connection.commit();
    console.log(`📦 [Sync] Processado lote ${Math.floor(i / BATCH_SIZE) + 1} de ${Math.ceil(parceiros.length / BATCH_SIZE)}`);
  }

  console.log(`✅ [Sync] UPSERT concluído - Inseridos: ${inseridos}, Atualizados: ${atualizados}`);
  return { inseridos, atualizados };
}

/**
 * Sincroniza parceiros de uma empresa específica
 */
export async function sincronizarParceirosPorEmpresa(idSistema: number, empresaNome: string): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;

  try {
    console.log(`🚀 [Sync] Iniciando sincronização de parceiros - ID_SISTEMA: ${idSistema}, Empresa: ${empresaNome}`);

    // Obter token específico para esta empresa
    await obterToken(idSistema);

    // Buscar parceiros do Sankhya
    const parceiros = await buscarParceirosSankhya(idSistema);

    // Conectar ao Oracle
    connection = await getOracleConnection();

    // Fase 1: Soft Delete (marcar todos como não atuais)
    const registrosDeletados = await marcarTodosComoNaoAtuais(connection, idSistema);

    // Fase 2: UPSERT (inserir/atualizar)
    const { inseridos, atualizados } = await upsertParceiros(connection, idSistema, parceiros);

    // Commit final
    await connection.commit();

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    console.log(`✅ [Sync] Sincronização concluída com sucesso para ${empresaNome}`);
    console.log(`📊 [Sync] Resumo: ${parceiros.length} registros, ${inseridos} inseridos, ${atualizados} atualizados, ${registrosDeletados} deletados`);
    console.log(`⏱️ [Sync] Duração: ${duracao}ms`);

    return {
      success: true,
      idSistema,
      empresa: empresaNome,
      totalRegistros: parceiros.length,
      registrosInseridos: inseridos,
      registrosAtualizados: atualizados,
      registrosDeletados,
      dataInicio,
      dataFim,
      duracao
    };

  } catch (error: any) {
    console.error(`❌ [Sync] Erro ao sincronizar parceiros para ${empresaNome}:`, error);

    if (connection) {
      try {
        await connection.rollback();
      } catch (rollbackError) {
        console.error('❌ [Sync] Erro ao fazer rollback:', rollbackError);
      }
    }

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    return {
      success: false,
      idSistema,
      empresa: empresaNome,
      totalRegistros: 0,
      registrosInseridos: 0,
      registrosAtualizados: 0,
      registrosDeletados: 0,
      dataInicio,
      dataFim,
      duracao,
      erro: error.message
    };

  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (closeError) {
        console.error('❌ [Sync] Erro ao fechar conexão:', closeError);
      }
    }
  }
}

/**
 * Sincroniza parceiros de todas as empresas ativas
 */
export async function sincronizarTodasEmpresas(): Promise<SyncResult[]> {
  console.log('🌐 [Sync] Iniciando sincronização de todas as empresas...');

  let connection: oracledb.Connection | undefined;
  const resultados: SyncResult[] = [];

  try {
    connection = await getOracleConnection();

    // Buscar todas as empresas ativas
    const result = await connection.execute(
      `SELECT ID_EMPRESA, EMPRESA FROM AD_CONTRATOS WHERE ATIVO = 'S' ORDER BY EMPRESA`,
      [],
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    await connection.close();
    connection = undefined;

    if (!result.rows || result.rows.length === 0) {
      console.log('⚠️ [Sync] Nenhuma empresa ativa encontrada');
      return [];
    }

    const empresas = result.rows as any[];
    console.log(`📋 [Sync] ${empresas.length} empresas ativas encontradas`);

    // Sincronizar cada empresa sequencialmente (uma por vez)
    for (const empresa of empresas) {
      console.log(`🔄 [Sync] Sincronizando empresa ${empresa.EMPRESA} (${empresa.ID_EMPRESA})...`);
      
      const resultado = await sincronizarParceirosPorEmpresa(
        empresa.ID_EMPRESA,
        empresa.EMPRESA
      );
      resultados.push(resultado);

      console.log(`✓ [Sync] Empresa ${empresa.EMPRESA} concluída`);
      
      // Aguardar 3 segundos entre sincronizações para evitar sobrecarga
      await new Promise(resolve => setTimeout(resolve, 3000));
    }

    const sucessos = resultados.filter(r => r.success).length;
    const falhas = resultados.filter(r => !r.success).length;

    console.log(`🏁 [Sync] Sincronização de todas as empresas concluída`);
    console.log(`✅ Sucessos: ${sucessos}, ❌ Falhas: ${falhas}`);

    return resultados;

  } catch (error: any) {
    console.error('❌ [Sync] Erro ao sincronizar todas as empresas:', error);
    throw error;
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (closeError) {
        console.error('❌ [Sync] Erro ao fechar conexão:', closeError);
      }
    }
  }
}

/**
 * Obter estatísticas de sincronização
 */
export async function obterEstatisticasSincronizacao(idSistema?: number) {
  const connection = await getOracleConnection();

  try {
    const whereClause = idSistema ? `WHERE ID_SISTEMA = :idSistema` : '';
    const params = idSistema ? { idSistema } : {};

    const result = await connection.execute(
      `SELECT 
         ID_SISTEMA,
         COUNT(*) as TOTAL_REGISTROS,
         SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN 1 ELSE 0 END) as REGISTROS_ATIVOS,
         SUM(CASE WHEN SANKHYA_ATUAL = 'N' THEN 1 ELSE 0 END) as REGISTROS_DELETADOS,
         MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO
       FROM AS_PARCEIROS
       ${whereClause}
       GROUP BY ID_SISTEMA
       ORDER BY ID_SISTEMA`,
      params,
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    return result.rows || [];
  } finally {
    await connection.close();
  }
}
