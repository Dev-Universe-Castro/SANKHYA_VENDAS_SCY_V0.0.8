import { NextResponse } from "next/server"
import { criarContrato } from "@/lib/oracle-service"

export async function POST(request: Request) {
  try {
    const body = await request.json()
    const { EMPRESA, CNPJ, SANKHYA_TOKEN, SANKHYA_APPKEY, SANKHYA_USERNAME, SANKHYA_PASSWORD, GEMINI_API_KEY, ATIVO } = body

    if (!EMPRESA || !CNPJ) {
      return NextResponse.json({ error: "Campos obrigatórios não preenchidos" }, { status: 400 })
    }

    const idEmpresa = await criarContrato({
      EMPRESA,
      CNPJ,
      SANKHYA_TOKEN: SANKHYA_TOKEN || '',
      SANKHYA_APPKEY: SANKHYA_APPKEY || '',
      SANKHYA_USERNAME: SANKHYA_USERNAME || '',
      SANKHYA_PASSWORD: SANKHYA_PASSWORD || '',
      GEMINI_API_KEY: GEMINI_API_KEY || '',
      ATIVO: ATIVO !== false
    })

    return NextResponse.json({ success: true, id: idEmpresa })
  } catch (error: any) {
    console.error("Erro ao criar contrato:", error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}