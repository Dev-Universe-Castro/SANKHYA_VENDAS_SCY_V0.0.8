import { NextResponse } from "next/server"
import { atualizarContrato } from "@/lib/oracle-service"

export async function POST(request: Request) {
  try {
    const body = await request.json()
    const { ID_EMPRESA, EMPRESA, CNPJ, SANKHYA_TOKEN, SANKHYA_APPKEY, SANKHYA_USERNAME, SANKHYA_PASSWORD, GEMINI_API_KEY, ATIVO } = body

    if (!ID_EMPRESA || !EMPRESA || !CNPJ) {
      return NextResponse.json({ error: "Campos obrigatórios não preenchidos" }, { status: 400 })
    }

    await atualizarContrato(ID_EMPRESA, {
      EMPRESA,
      CNPJ,
      SANKHYA_TOKEN: SANKHYA_TOKEN || '',
      SANKHYA_APPKEY: SANKHYA_APPKEY || '',
      SANKHYA_USERNAME: SANKHYA_USERNAME || '',
      SANKHYA_PASSWORD: SANKHYA_PASSWORD || '',
      GEMINI_API_KEY: GEMINI_API_KEY || '',
      ATIVO: ATIVO !== false
    })

    return NextResponse.json({ success: true })
  } catch (error: any) {
    console.error("Erro ao atualizar contrato:", error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}