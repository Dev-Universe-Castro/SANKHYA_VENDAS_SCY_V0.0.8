
"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { authService } from "@/lib/auth-service"
import DashboardLayout from "@/components/dashboard-layout"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { useToast } from "@/hooks/use-toast"
import { RefreshCw, Clock, Database, Settings, Tag } from "lucide-react"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import { Label } from "@/components/ui/label"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"

interface Contrato {
  ID_EMPRESA: number
  EMPRESA: string
  CNPJ: string
  ATIVO: boolean
}

interface EstatisticaSync {
  ID_SISTEMA: number
  TOTAL_REGISTROS: number
  REGISTROS_ATIVOS: number
  REGISTROS_DELETADOS: number
  ULTIMA_SINCRONIZACAO: string
}

interface SyncResult {
  success: boolean
  idSistema: number
  empresa: string
  totalRegistros: number
  registrosInseridos: number
  registrosAtualizados: number
  registrosDeletados: number
  dataInicio: string
  dataFim: string
  duracao: number
  erro?: string
}

interface AutoSyncConfig {
  idSistema: number
  enabled: boolean
  intervalMinutes: number
  intervalId?: NodeJS.Timeout
  nextSyncTime?: Date
}

export default function SincronizacaoTiposNegociacaoPage() {
  const router = useRouter()
  const { toast } = useToast()
  const [contratos, setContratos] = useState<Contrato[]>([])
  const [estatisticas, setEstatisticas] = useState<Map<number, EstatisticaSync>>(new Map())
  const [loading, setLoading] = useState(true)
  const [syncingAll, setSyncingAll] = useState(false)
  const [syncingOne, setSyncingOne] = useState<number | null>(null)
  
  const [autoSyncConfigs, setAutoSyncConfigs] = useState<Map<number, AutoSyncConfig>>(new Map())
  const [configModalOpen, setConfigModalOpen] = useState(false)
  const [selectedEmpresa, setSelectedEmpresa] = useState<Contrato | null>(null)

  useEffect(() => {
    const currentUser = authService.getCurrentUser()
    if (!currentUser || currentUser.role !== 'Administrador') {
      router.push("/dashboard")
      return
    }
    loadData()
  }, [router])

  useEffect(() => {
    return () => {
      autoSyncConfigs.forEach(config => {
        if (config.intervalId) {
          clearInterval(config.intervalId)
        }
      })
    }
  }, [autoSyncConfigs])

  const loadData = async () => {
    try {
      setLoading(true)
      
      const contratosRes = await fetch('/api/contratos')
      if (contratosRes.ok) {
        const contratosData = await contratosRes.json()
        setContratos(contratosData.filter((c: Contrato) => c.ATIVO))
      }

      const statsRes = await fetch('/api/sync/tipos-negociacao')
      if (statsRes.ok) {
        const statsData = await statsRes.json()
        const statsMap = new Map<number, EstatisticaSync>()
        statsData.forEach((stat: EstatisticaSync) => {
          statsMap.set(stat.ID_SISTEMA, stat)
        })
        setEstatisticas(statsMap)
      }
    } catch (error) {
      console.error('Erro ao carregar dados:', error)
      toast({
        title: "Erro",
        description: "Erro ao carregar dados de sincronização",
        variant: "destructive"
      })
    } finally {
      setLoading(false)
    }
  }

  const sincronizarTodas = async () => {
    try {
      setSyncingAll(true)
      toast({
        title: "Sincronização iniciada",
        description: "Sincronizando tipos de negociação de todas as empresas..."
      })

      const response = await fetch('/api/sync/tipos-negociacao', {
        method: 'POST'
      })

      if (!response.ok) {
        throw new Error('Erro ao sincronizar')
      }

      const resultados: SyncResult[] = await response.json()
      
      const sucessos = resultados.filter(r => r.success).length
      const falhas = resultados.filter(r => !r.success).length

      toast({
        title: "Sincronização concluída",
        description: `${sucessos} empresas sincronizadas. ${falhas > 0 ? `${falhas} falhas.` : ''}`,
        variant: falhas > 0 ? "destructive" : "default"
      })

      await loadData()
    } catch (error) {
      console.error('Erro ao sincronizar:', error)
      toast({
        title: "Erro",
        description: "Erro ao sincronizar empresas",
        variant: "destructive"
      })
    } finally {
      setSyncingAll(false)
    }
  }

  const sincronizarEmpresa = async (idSistema: number, empresa: string) => {
    try {
      setSyncingOne(idSistema)
      toast({
        title: "Sincronização iniciada",
        description: `Sincronizando tipos de negociação de ${empresa}...`
      })

      const response = await fetch(
        `/api/sync/tipos-negociacao?idSistema=${idSistema}&empresa=${encodeURIComponent(empresa)}`,
        { method: 'POST' }
      )

      if (!response.ok) {
        throw new Error('Erro ao sincronizar')
      }

      const resultado: SyncResult = await response.json()

      if (resultado.success) {
        toast({
          title: "Sincronização concluída",
          description: `${resultado.totalRegistros} registros processados`,
        })
      } else {
        toast({
          title: "Erro na sincronização",
          description: resultado.erro || "Erro desconhecido",
          variant: "destructive"
        })
      }

      await loadData()
    } catch (error) {
      console.error('Erro ao sincronizar:', error)
      toast({
        title: "Erro",
        description: "Erro ao sincronizar empresa",
        variant: "destructive"
      })
    } finally {
      setSyncingOne(null)
    }
  }

  const openConfigModal = (contrato: Contrato) => {
    setSelectedEmpresa(contrato)
    setConfigModalOpen(true)
  }

  const handleAutoSyncToggle = (idSistema: number, enabled: boolean) => {
    const config = autoSyncConfigs.get(idSistema) || {
      idSistema,
      enabled: false,
      intervalMinutes: 60
    }

    if (enabled) {
      const intervalMs = config.intervalMinutes * 60 * 1000
      
      const intervalId = setInterval(() => {
        const contrato = contratos.find(c => c.ID_EMPRESA === idSistema)
        if (contrato) {
          sincronizarEmpresa(idSistema, contrato.EMPRESA)
        }
      }, intervalMs)
      
      const nextSync = new Date()
      nextSync.setMinutes(nextSync.getMinutes() + config.intervalMinutes)
      
      const newConfig: AutoSyncConfig = {
        ...config,
        enabled: true,
        intervalId,
        nextSyncTime: nextSync
      }
      
      setAutoSyncConfigs(new Map(autoSyncConfigs.set(idSistema, newConfig)))
      
      const contrato = contratos.find(c => c.ID_EMPRESA === idSistema)
      toast({
        title: "Sincronização automática ativada",
        description: `${contrato?.EMPRESA} a cada ${config.intervalMinutes} minutos`,
      })
    } else {
      if (config.intervalId) {
        clearInterval(config.intervalId)
      }
      
      const newConfig: AutoSyncConfig = {
        ...config,
        enabled: false,
        intervalId: undefined,
        nextSyncTime: undefined
      }
      
      setAutoSyncConfigs(new Map(autoSyncConfigs.set(idSistema, newConfig)))
      
      toast({
        title: "Sincronização automática desativada",
      })
    }
  }

  const handleIntervalChange = (idSistema: number, intervalMinutes: number) => {
    const config = autoSyncConfigs.get(idSistema) || {
      idSistema,
      enabled: false,
      intervalMinutes: 60
    }

    if (config.enabled && config.intervalId) {
      clearInterval(config.intervalId)
      
      const intervalMs = intervalMinutes * 60 * 1000
      const intervalId = setInterval(() => {
        const contrato = contratos.find(c => c.ID_EMPRESA === idSistema)
        if (contrato) {
          sincronizarEmpresa(idSistema, contrato.EMPRESA)
        }
      }, intervalMs)
      
      const nextSync = new Date()
      nextSync.setMinutes(nextSync.getMinutes() + intervalMinutes)
      
      const newConfig: AutoSyncConfig = {
        ...config,
        intervalMinutes,
        intervalId,
        nextSyncTime: nextSync
      }
      
      setAutoSyncConfigs(new Map(autoSyncConfigs.set(idSistema, newConfig)))
    } else {
      const newConfig: AutoSyncConfig = {
        ...config,
        intervalMinutes
      }
      
      setAutoSyncConfigs(new Map(autoSyncConfigs.set(idSistema, newConfig)))
    }
  }

  const formatarData = (dataStr: string) => {
    if (!dataStr) return 'Nunca sincronizado'
    const data = new Date(dataStr)
    return data.toLocaleString('pt-BR')
  }

  if (loading) {
    return (
      <DashboardLayout>
        <div className="flex items-center justify-center h-96">
          <RefreshCw className="w-8 h-8 animate-spin text-primary" />
        </div>
      </DashboardLayout>
    )
  }

  return (
    <DashboardLayout>
      <div className="space-y-6 p-6">
        <div className="flex justify-between items-center">
          <div>
            <h1 className="text-3xl font-bold">Sincronização de Tipos de Negociação</h1>
            <p className="text-muted-foreground mt-2">
              Gerencie a sincronização de tipos de negociação entre Sankhya e o sistema local
            </p>
          </div>
          <Button
            onClick={sincronizarTodas}
            disabled={syncingAll || syncingOne !== null}
            className="gap-2"
          >
            {syncingAll ? (
              <>
                <RefreshCw className="w-4 h-4 animate-spin" />
                Sincronizando...
              </>
            ) : (
              <>
                <RefreshCw className="w-4 h-4" />
                Sincronizar Todas
              </>
            )}
          </Button>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Empresas Cadastradas</CardTitle>
            <CardDescription>
              Status de sincronização e configuração de agendamento por empresa
            </CardDescription>
          </CardHeader>
          <CardContent>
            {contratos.length === 0 ? (
              <div className="text-center py-12">
                <Tag className="w-16 h-16 mx-auto mb-4 text-muted-foreground opacity-50" />
                <p className="text-lg font-medium">Nenhuma empresa ativa encontrada</p>
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>ID</TableHead>
                    <TableHead>Empresa</TableHead>
                    <TableHead>CNPJ</TableHead>
                    <TableHead className="text-center">Total</TableHead>
                    <TableHead className="text-center">Ativos</TableHead>
                    <TableHead>Última Sync</TableHead>
                    <TableHead className="text-center">Auto-Sync</TableHead>
                    <TableHead className="text-right">Ações</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {contratos.map((contrato) => {
                    const stats = estatisticas.get(contrato.ID_EMPRESA)
                    const isSyncing = syncingOne === contrato.ID_EMPRESA
                    const autoSyncConfig = autoSyncConfigs.get(contrato.ID_EMPRESA)
                    const isAutoSyncActive = autoSyncConfig?.enabled || false

                    return (
                      <TableRow key={contrato.ID_EMPRESA}>
                        <TableCell>
                          <Badge variant="outline">{contrato.ID_EMPRESA}</Badge>
                        </TableCell>
                        <TableCell className="font-medium">{contrato.EMPRESA}</TableCell>
                        <TableCell className="text-sm text-muted-foreground">
                          {contrato.CNPJ}
                        </TableCell>
                        <TableCell className="text-center">
                          {stats ? (
                            <div className="flex items-center justify-center gap-1">
                              <Database className="w-4 h-4 text-muted-foreground" />
                              <span className="font-semibold">{stats.TOTAL_REGISTROS}</span>
                            </div>
                          ) : (
                            <span className="text-muted-foreground">-</span>
                          )}
                        </TableCell>
                        <TableCell className="text-center">
                          {stats ? (
                            <span className="font-semibold text-green-600">
                              {stats.REGISTROS_ATIVOS}
                            </span>
                          ) : (
                            <span className="text-muted-foreground">-</span>
                          )}
                        </TableCell>
                        <TableCell>
                          <div className="flex flex-col gap-1">
                            <div className="flex items-center gap-2 text-xs">
                              <Clock className="w-3 h-3 text-muted-foreground" />
                              <span className="text-muted-foreground">
                                {stats ? formatarData(stats.ULTIMA_SINCRONIZACAO) : 'Nunca'}
                              </span>
                            </div>
                            {isAutoSyncActive && autoSyncConfig?.nextSyncTime && (
                              <div className="flex items-center gap-2 text-xs text-blue-600">
                                <Clock className="w-3 h-3" />
                                <span>
                                  Próxima: {formatarData(autoSyncConfig.nextSyncTime.toISOString())}
                                </span>
                              </div>
                            )}
                          </div>
                        </TableCell>
                        <TableCell className="text-center">
                          <div className="flex items-center justify-center gap-2">
                            {isAutoSyncActive && (
                              <Badge variant="default" className="text-xs">
                                {autoSyncConfig?.intervalMinutes}min
                              </Badge>
                            )}
                            <Button
                              onClick={() => openConfigModal(contrato)}
                              size="sm"
                              variant="ghost"
                              className="h-8 w-8 p-0"
                            >
                              <Settings className="w-4 h-4" />
                            </Button>
                          </div>
                        </TableCell>
                        <TableCell className="text-right">
                          <Button
                            onClick={() => sincronizarEmpresa(contrato.ID_EMPRESA, contrato.EMPRESA)}
                            disabled={isSyncing || syncingAll}
                            size="sm"
                            variant="outline"
                          >
                            {isSyncing ? (
                              <>
                                <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
                                Sincronizando...
                              </>
                            ) : (
                              <>
                                <RefreshCw className="w-4 h-4 mr-2" />
                                Sincronizar
                              </>
                            )}
                          </Button>
                        </TableCell>
                      </TableRow>
                    )
                  })}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>

        <Dialog open={configModalOpen} onOpenChange={setConfigModalOpen}>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Configurar Sincronização Automática</DialogTitle>
              <DialogDescription>
                {selectedEmpresa?.EMPRESA}
              </DialogDescription>
            </DialogHeader>
            
            {selectedEmpresa && (
              <div className="space-y-4 py-4">
                <div className="flex items-center justify-between">
                  <div className="space-y-1">
                    <Label htmlFor="auto-sync-toggle" className="text-base font-medium">
                      Ativar sincronização automática
                    </Label>
                    <p className="text-sm text-muted-foreground">
                      Sincroniza automaticamente os tipos de negociação
                    </p>
                  </div>
                  <Switch
                    id="auto-sync-toggle"
                    checked={autoSyncConfigs.get(selectedEmpresa.ID_EMPRESA)?.enabled || false}
                    onCheckedChange={(enabled) =>
                      handleAutoSyncToggle(selectedEmpresa.ID_EMPRESA, enabled)
                    }
                  />
                </div>

                <div className="space-y-2">
                  <Label htmlFor="interval-select">Intervalo de sincronização</Label>
                  <Select
                    value={String(autoSyncConfigs.get(selectedEmpresa.ID_EMPRESA)?.intervalMinutes || 60)}
                    onValueChange={(value) =>
                      handleIntervalChange(selectedEmpresa.ID_EMPRESA, parseInt(value))
                    }
                  >
                    <SelectTrigger id="interval-select">
                      <SelectValue placeholder="Selecione o intervalo" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="30">30 minutos</SelectItem>
                      <SelectItem value="60">1 hora</SelectItem>
                      <SelectItem value="120">2 horas</SelectItem>
                      <SelectItem value="180">3 horas</SelectItem>
                      <SelectItem value="360">6 horas</SelectItem>
                      <SelectItem value="720">12 horas</SelectItem>
                      <SelectItem value="1440">24 horas</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                {autoSyncConfigs.get(selectedEmpresa.ID_EMPRESA)?.enabled && 
                 autoSyncConfigs.get(selectedEmpresa.ID_EMPRESA)?.nextSyncTime && (
                  <div className="bg-blue-50 dark:bg-blue-950 p-3 rounded-lg">
                    <div className="flex items-center gap-2 text-sm text-blue-600 dark:text-blue-400">
                      <Clock className="w-4 h-4" />
                      <span>
                        Próxima sincronização:{' '}
                        {formatarData(
                          autoSyncConfigs.get(selectedEmpresa.ID_EMPRESA)!.nextSyncTime!.toISOString()
                        )}
                      </span>
                    </div>
                  </div>
                )}
              </div>
            )}
          </DialogContent>
        </Dialog>
      </div>
    </DashboardLayout>
  )
}
