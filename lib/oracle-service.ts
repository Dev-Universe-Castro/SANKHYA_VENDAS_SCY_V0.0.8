
import oracledb from 'oracledb';

interface OracleConfig {
  user: string;
  password: string;
  connectString: string;
}

let pool: oracledb.Pool | null = null;

export async function initOraclePool(config: OracleConfig) {
  if (pool) {
    console.log('♻️ Reutilizando pool Oracle existente');
    return pool;
  }

  console.log('🔧 Criando novo pool Oracle com configurações:', {
    user: config.user,
    connectString: config.connectString,
    hasPassword: !!config.password,
    passwordLength: config.password?.length
  });

  try {
    pool = await oracledb.createPool({
      user: config.user,
      password: config.password,
      connectString: config.connectString,
      poolMin: 1,
      poolMax: 5,
      poolIncrement: 1,
      poolTimeout: 60,
      connectionTimeout: 60000
    });
    
    console.log('✅ Pool Oracle criado com sucesso');
    
    // Testar conexão
    const testConn = await pool.getConnection();
    console.log('✅ Conexão de teste bem-sucedida');
    await testConn.close();
    
    return pool;
  } catch (error: any) {
    console.error('❌ Erro ao criar pool Oracle:', {
      message: error.message,
      code: error.errorNum,
      offset: error.offset
    });
    throw error;
  }
}

export async function getOracleConnection() {
  if (!pool) {
    const config: OracleConfig = {
      user: process.env.ORACLE_USER || '',
      password: process.env.ORACLE_PASSWORD || '',
      connectString: process.env.ORACLE_CONNECT_STRING || ''
    };
    
    console.log('🔑 Credenciais Oracle carregadas:', {
      user: config.user,
      connectString: config.connectString,
      hasPassword: !!config.password
    });
    
    await initOraclePool(config);
  }
  
  return pool!.getConnection();
}

export async function closeOraclePool() {
  if (pool) {
    await pool.close(10);
    pool = null;
    console.log('✅ Pool Oracle fechado');
  }
}

export interface Contrato {
  ID_EMPRESA: number;
  EMPRESA: string;
  CNPJ: string;
  SANKHYA_TOKEN: string;
  SANKHYA_APPKEY: string;
  SANKHYA_USERNAME: string;
  SANKHYA_PASSWORD: string;
  GEMINI_API_KEY: string;
  ATIVO: boolean;
  DATA_CRIACAO?: Date;
  DATA_ATUALIZACAO?: Date;
}

export async function listarContratos(): Promise<Contrato[]> {
  const connection = await getOracleConnection();
  
  try {
    const result = await connection.execute(
      `SELECT 
        ID_EMPRESA, 
        EMPRESA, 
        CNPJ, 
        SANKHYA_TOKEN, 
        SANKHYA_APPKEY, 
        SANKHYA_USERNAME, 
        SANKHYA_PASSWORD, 
        GEMINI_API_KEY, 
        ATIVO,
        DATA_CRIACAO,
        DATA_ATUALIZACAO
      FROM AD_CONTRATOS 
      ORDER BY EMPRESA ASC`,
      [],
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );
    
    const contratos = (result.rows as any[]).map(row => ({
      ID_EMPRESA: row.ID_EMPRESA,
      EMPRESA: row.EMPRESA,
      CNPJ: row.CNPJ,
      SANKHYA_TOKEN: row.SANKHYA_TOKEN || '',
      SANKHYA_APPKEY: row.SANKHYA_APPKEY || '',
      SANKHYA_USERNAME: row.SANKHYA_USERNAME || '',
      SANKHYA_PASSWORD: row.SANKHYA_PASSWORD || '',
      GEMINI_API_KEY: row.GEMINI_API_KEY || '',
      ATIVO: row.ATIVO === 'S',
      DATA_CRIACAO: row.DATA_CRIACAO,
      DATA_ATUALIZACAO: row.DATA_ATUALIZACAO
    }));
    
    return contratos;
  } finally {
    await connection.close();
  }
}

export async function criarContrato(contrato: Omit<Contrato, 'ID_EMPRESA'>): Promise<number> {
  console.log('📝 Iniciando criação de contrato:', { empresa: contrato.EMPRESA, cnpj: contrato.CNPJ });
  
  let connection;
  try {
    connection = await getOracleConnection();
    console.log('✅ Conexão Oracle obtida');
    
    const result = await connection.execute(
      `INSERT INTO AD_CONTRATOS 
        (EMPRESA, CNPJ, SANKHYA_TOKEN, SANKHYA_APPKEY, SANKHYA_USERNAME, SANKHYA_PASSWORD, GEMINI_API_KEY, ATIVO)
      VALUES 
        (:empresa, :cnpj, :token, :appkey, :username, :password, :gemini, :ativo)
      RETURNING ID_EMPRESA INTO :id`,
      {
        empresa: contrato.EMPRESA,
        cnpj: contrato.CNPJ,
        token: contrato.SANKHYA_TOKEN || null,
        appkey: contrato.SANKHYA_APPKEY || null,
        username: contrato.SANKHYA_USERNAME || null,
        password: contrato.SANKHYA_PASSWORD || null,
        gemini: contrato.GEMINI_API_KEY || null,
        ativo: contrato.ATIVO ? 'S' : 'N',
        id: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }
      },
      { autoCommit: true }
    );
    
    const idEmpresa = (result.outBinds as any).id[0];
    console.log('✅ Contrato criado com ID:', idEmpresa);
    return idEmpresa;
  } catch (error: any) {
    console.error('❌ Erro ao criar contrato:', {
      message: error.message,
      code: error.errorNum,
      offset: error.offset
    });
    throw error;
  } finally {
    if (connection) {
      try {
        await connection.close();
        console.log('✅ Conexão Oracle fechada');
      } catch (err) {
        console.error('⚠️ Erro ao fechar conexão:', err);
      }
    }
  }
}

export async function atualizarContrato(id: number, contrato: Partial<Contrato>): Promise<void> {
  const connection = await getOracleConnection();
  
  try {
    await connection.execute(
      `UPDATE AD_CONTRATOS 
      SET 
        EMPRESA = :empresa,
        CNPJ = :cnpj,
        SANKHYA_TOKEN = :token,
        SANKHYA_APPKEY = :appkey,
        SANKHYA_USERNAME = :username,
        SANKHYA_PASSWORD = :password,
        GEMINI_API_KEY = :gemini,
        ATIVO = :ativo
      WHERE ID_EMPRESA = :id`,
      {
        id,
        empresa: contrato.EMPRESA,
        cnpj: contrato.CNPJ,
        token: contrato.SANKHYA_TOKEN || null,
        appkey: contrato.SANKHYA_APPKEY || null,
        username: contrato.SANKHYA_USERNAME || null,
        password: contrato.SANKHYA_PASSWORD || null,
        gemini: contrato.GEMINI_API_KEY || null,
        ativo: contrato.ATIVO ? 'S' : 'N'
      },
      { autoCommit: true }
    );
  } finally {
    await connection.close();
  }
}

export async function deletarContrato(id: number): Promise<void> {
  const connection = await getOracleConnection();
  
  try {
    await connection.execute(
      `DELETE FROM AD_CONTRATOS WHERE ID_EMPRESA = :id`,
      { id },
      { autoCommit: true }
    );
  } finally {
    await connection.close();
  }
}

export async function buscarContratoPorId(id: number): Promise<Contrato | null> {
  const connection = await getOracleConnection();
  
  try {
    const result = await connection.execute(
      `SELECT 
        ID_EMPRESA, 
        EMPRESA, 
        CNPJ, 
        SANKHYA_TOKEN, 
        SANKHYA_APPKEY, 
        SANKHYA_USERNAME, 
        SANKHYA_PASSWORD, 
        GEMINI_API_KEY, 
        ATIVO
      FROM AD_CONTRATOS 
      WHERE ID_EMPRESA = :id`,
      { id },
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );
    
    if (!result.rows || result.rows.length === 0) {
      return null;
    }
    
    const row = result.rows[0] as any;
    
    return {
      ID_EMPRESA: row.ID_EMPRESA,
      EMPRESA: row.EMPRESA,
      CNPJ: row.CNPJ,
      SANKHYA_TOKEN: row.SANKHYA_TOKEN || '',
      SANKHYA_APPKEY: row.SANKHYA_APPKEY || '',
      SANKHYA_USERNAME: row.SANKHYA_USERNAME || '',
      SANKHYA_PASSWORD: row.SANKHYA_PASSWORD || '',
      GEMINI_API_KEY: row.GEMINI_API_KEY || '',
      ATIVO: row.ATIVO === 'S'
    };
  } finally {
    await connection.close();
  }
}
