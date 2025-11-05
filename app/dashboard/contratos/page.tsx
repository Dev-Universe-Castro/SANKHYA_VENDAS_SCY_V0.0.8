
"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { authService } from "@/lib/auth-service"
import DashboardLayout from "@/components/dashboard-layout"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Badge } from "@/components/ui/badge"
import { toast } from "sonner"
import { Plus, Edit, Trash2, Eye, EyeOff } from "lucide-react"
import { Switch } from "@/components/ui/switch"

interface Contrato {
  ID_EMPRESA: number
  EMPRESA: string
  CNPJ: string
  SANKHYA_TOKEN: string
  SANKHYA_APPKEY: string
  SANKHYA_USERNAME: string
  SANKHYA_PASSWORD: string
  GEMINI_API_KEY: string
  ATIVO: boolean
}

export default function ContratosPage() {
  const router = useRouter()
  const [contratos, setContratos] = useState<Contrato[]>([])
  const [isModalOpen, setIsModalOpen] = useState(false)
  const [isEditing, setIsEditing] = useState(false)
  const [currentContrato, setCurrentContrato] = useState<Contrato | null>(null)
  const [showPasswords, setShowPasswords] = useState<{ [key: string]: boolean }>({})
  const [formData, setFormData] = useState<Partial<Contrato>>({
    EMPRESA: "",
    CNPJ: "",
    SANKHYA_TOKEN: "",
    SANKHYA_APPKEY: "",
    SANKHYA_USERNAME: "",
    SANKHYA_PASSWORD: "",
    GEMINI_API_KEY: "",
    ATIVO: true
  })

  useEffect(() => {
    const currentUser = authService.getCurrentUser()
    if (!currentUser || currentUser.role !== "Administrador") {
      router.push("/dashboard")
      return
    }
    carregarContratos()
  }, [router])

  const carregarContratos = async () => {
    try {
      const response = await fetch("/api/contratos")
      if (response.ok) {
        const data = await response.json()
        setContratos(data)
      }
    } catch (error) {
      console.error("Erro ao carregar contratos:", error)
    }
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()

    if (!formData.EMPRESA || !formData.CNPJ) {
      toast.error("Preencha os campos obrigatórios")
      return
    }

    try {
      const url = isEditing ? "/api/contratos/atualizar" : "/api/contratos/criar"
      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(isEditing ? { ...formData, ID_EMPRESA: currentContrato?.ID_EMPRESA } : formData)
      })

      if (response.ok) {
        toast.success(isEditing ? "Contrato atualizado com sucesso" : "Contrato criado com sucesso")
        setIsModalOpen(false)
        resetForm()
        carregarContratos()
      } else {
        const error = await response.json()
        toast.error(error.error || "Erro ao salvar contrato")
      }
    } catch (error) {
      toast.error("Erro ao salvar contrato")
    }
  }

  const handleEdit = (contrato: Contrato) => {
    setCurrentContrato(contrato)
    setFormData(contrato)
    setIsEditing(true)
    setIsModalOpen(true)
  }

  const handleDelete = async (id: number) => {
    if (!confirm("Deseja realmente excluir este contrato?")) return

    try {
      const response = await fetch("/api/contratos/deletar", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ID_EMPRESA: id })
      })

      if (response.ok) {
        toast.success("Contrato excluído com sucesso")
        carregarContratos()
      } else {
        toast.error("Erro ao excluir contrato")
      }
    } catch (error) {
      toast.error("Erro ao excluir contrato")
    }
  }

  const resetForm = () => {
    setFormData({
      EMPRESA: "",
      CNPJ: "",
      SANKHYA_TOKEN: "",
      SANKHYA_APPKEY: "",
      SANKHYA_USERNAME: "",
      SANKHYA_PASSWORD: "",
      GEMINI_API_KEY: "",
      ATIVO: true
    })
    setIsEditing(false)
    setCurrentContrato(null)
  }

  const togglePasswordVisibility = (field: string) => {
    setShowPasswords(prev => ({ ...prev, [field]: !prev[field] }))
  }

  const formatCNPJ = (value: string) => {
    return value
      .replace(/\D/g, "")
      .replace(/^(\d{2})(\d)/, "$1.$2")
      .replace(/^(\d{2})\.(\d{3})(\d)/, "$1.$2.$3")
      .replace(/\.(\d{3})(\d)/, ".$1/$2")
      .replace(/(\d{4})(\d)/, "$1-$2")
      .slice(0, 18)
  }

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold tracking-tight">Contratos</h1>
            <p className="text-muted-foreground">Gerencie os contratos e credenciais das empresas</p>
          </div>
          <Button onClick={() => { resetForm(); setIsModalOpen(true) }}>
            <Plus className="w-4 h-4 mr-2" />
            Novo Contrato
          </Button>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Contratos Cadastrados</CardTitle>
            <CardDescription>Lista de todas as empresas contratantes</CardDescription>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>ID</TableHead>
                  <TableHead>Empresa</TableHead>
                  <TableHead>CNPJ</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="text-right">Ações</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {contratos.map((contrato) => (
                  <TableRow key={contrato.ID_EMPRESA}>
                    <TableCell>{contrato.ID_EMPRESA}</TableCell>
                    <TableCell className="font-medium">{contrato.EMPRESA}</TableCell>
                    <TableCell>{contrato.CNPJ}</TableCell>
                    <TableCell>
                      <Badge variant={contrato.ATIVO ? "default" : "secondary"}>
                        {contrato.ATIVO ? "Ativo" : "Inativo"}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-right">
                      <Button variant="ghost" size="sm" onClick={() => handleEdit(contrato)}>
                        <Edit className="w-4 h-4" />
                      </Button>
                      <Button variant="ghost" size="sm" onClick={() => handleDelete(contrato.ID_EMPRESA)}>
                        <Trash2 className="w-4 h-4" />
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
                {contratos.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={5} className="text-center text-muted-foreground">
                      Nenhum contrato cadastrado
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        <Dialog open={isModalOpen} onOpenChange={setIsModalOpen}>
          <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
            <DialogHeader>
              <DialogTitle>{isEditing ? "Editar Contrato" : "Novo Contrato"}</DialogTitle>
              <DialogDescription>
                Preencha as informações da empresa e suas credenciais
              </DialogDescription>
            </DialogHeader>

            <form onSubmit={handleSubmit} className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="empresa">Empresa *</Label>
                  <Input
                    id="empresa"
                    value={formData.EMPRESA}
                    onChange={(e) => setFormData({ ...formData, EMPRESA: e.target.value })}
                    required
                  />
                </div>

                <div className="space-y-2">
                  <Label htmlFor="cnpj">CNPJ *</Label>
                  <Input
                    id="cnpj"
                    value={formData.CNPJ}
                    onChange={(e) => setFormData({ ...formData, CNPJ: formatCNPJ(e.target.value) })}
                    required
                  />
                </div>
              </div>

              <div className="border-t pt-4">
                <h3 className="text-sm font-semibold mb-3">Credenciais Sankhya</h3>
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label htmlFor="sankhya_token">Token</Label>
                    <div className="relative">
                      <Input
                        id="sankhya_token"
                        type={showPasswords.token ? "text" : "password"}
                        value={formData.SANKHYA_TOKEN}
                        onChange={(e) => setFormData({ ...formData, SANKHYA_TOKEN: e.target.value })}
                      />
                      <Button
                        type="button"
                        variant="ghost"
                        size="sm"
                        className="absolute right-0 top-0 h-full"
                        onClick={() => togglePasswordVisibility("token")}
                      >
                        {showPasswords.token ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                      </Button>
                    </div>
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="sankhya_appkey">App Key</Label>
                    <div className="relative">
                      <Input
                        id="sankhya_appkey"
                        type={showPasswords.appkey ? "text" : "password"}
                        value={formData.SANKHYA_APPKEY}
                        onChange={(e) => setFormData({ ...formData, SANKHYA_APPKEY: e.target.value })}
                      />
                      <Button
                        type="button"
                        variant="ghost"
                        size="sm"
                        className="absolute right-0 top-0 h-full"
                        onClick={() => togglePasswordVisibility("appkey")}
                      >
                        {showPasswords.appkey ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                      </Button>
                    </div>
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="sankhya_username">Username</Label>
                    <Input
                      id="sankhya_username"
                      value={formData.SANKHYA_USERNAME}
                      onChange={(e) => setFormData({ ...formData, SANKHYA_USERNAME: e.target.value })}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="sankhya_password">Password</Label>
                    <div className="relative">
                      <Input
                        id="sankhya_password"
                        type={showPasswords.sankhya_password ? "text" : "password"}
                        value={formData.SANKHYA_PASSWORD}
                        onChange={(e) => setFormData({ ...formData, SANKHYA_PASSWORD: e.target.value })}
                      />
                      <Button
                        type="button"
                        variant="ghost"
                        size="sm"
                        className="absolute right-0 top-0 h-full"
                        onClick={() => togglePasswordVisibility("sankhya_password")}
                      >
                        {showPasswords.sankhya_password ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                      </Button>
                    </div>
                  </div>
                </div>
              </div>

              <div className="border-t pt-4">
                <h3 className="text-sm font-semibold mb-3">Credenciais Gemini</h3>
                <div className="space-y-2">
                  <Label htmlFor="gemini_api_key">API Key</Label>
                  <div className="relative">
                    <Input
                      id="gemini_api_key"
                      type={showPasswords.gemini ? "text" : "password"}
                      value={formData.GEMINI_API_KEY}
                      onChange={(e) => setFormData({ ...formData, GEMINI_API_KEY: e.target.value })}
                    />
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      className="absolute right-0 top-0 h-full"
                      onClick={() => togglePasswordVisibility("gemini")}
                    >
                      {showPasswords.gemini ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                    </Button>
                  </div>
                </div>
              </div>

              <div className="flex items-center space-x-2 border-t pt-4">
                <Switch
                  id="ativo"
                  checked={formData.ATIVO}
                  onCheckedChange={(checked) => setFormData({ ...formData, ATIVO: checked })}
                />
                <Label htmlFor="ativo">Contrato Ativo</Label>
              </div>

              <DialogFooter>
                <Button type="button" variant="outline" onClick={() => setIsModalOpen(false)}>
                  Cancelar
                </Button>
                <Button type="submit">
                  {isEditing ? "Atualizar" : "Cadastrar"}
                </Button>
              </DialogFooter>
            </form>
          </DialogContent>
        </Dialog>
      </div>
    </DashboardLayout>
  )
}
