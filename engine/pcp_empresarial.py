"""
Sistema PCP-TOC EMPRESARIAL
Versão para uso em empresas reais com importação de dados
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from enum import Enum
import matplotlib.pyplot as plt
import seaborn as sns
import json
from pathlib import Path

# Importar classes base do sistema original
from pcp_toc_jobshop import (
    StatusPedido, TipoBuffer, Operacao, Pedido, Recurso,
    GestorGargalo, GestorBuffer, SistemaPCP_TOC
)


class ImportadorDados:
    """Importa dados de Excel, CSV ou JSON para o sistema"""
    
    @staticmethod
    def importar_recursos_excel(caminho_arquivo: str) -> Dict[str, Recurso]:
        """
        Importa recursos de planilha Excel
        
        Formato esperado:
        | Recurso | Capacidade_Horas_Dia |
        |---------|---------------------|
        | Torno   | 16.0                |
        | Fresa   | 8.0                 |
        """
        df = pd.read_excel(caminho_arquivo, sheet_name='Recursos')
        recursos = {}
        
        for _, row in df.iterrows():
            nome = str(row['Recurso']).strip()
            capacidade = float(row['Capacidade_Horas_Dia'])
            recursos[nome] = Recurso(nome, capacidade)
        
        print(f"✓ {len(recursos)} recursos importados de {caminho_arquivo}")
        return recursos
    
    @staticmethod
    def importar_pedidos_excel(caminho_arquivo: str) -> List[Pedido]:
        """
        Importa pedidos de planilha Excel
        
        Formato esperado em aba 'Pedidos':
        | ID_Pedido | Cliente | Data_Entrega | Prioridade |
        |-----------|---------|--------------|------------|
        | PED-001   | ABC     | 2026-02-15   | 8          |
        
        Formato esperado em aba 'Operacoes':
        | ID_Pedido | ID_Operacao | Recurso | Setup_H | Proc_H | Ordem |
        |-----------|-------------|---------|---------|--------|-------|
        | PED-001   | OP-001-1    | Torno   | 0.5     | 2.0    | 1     |
        """
        # Ler abas
        df_pedidos = pd.read_excel(caminho_arquivo, sheet_name='Pedidos')
        df_operacoes = pd.read_excel(caminho_arquivo, sheet_name='Operacoes')
        
        pedidos = []
        data_atual = datetime.now()
        
        for _, row_ped in df_pedidos.iterrows():
            id_pedido = str(row_ped['ID_Pedido']).strip()
            
            # Buscar operações deste pedido
            ops_pedido = df_operacoes[df_operacoes['ID_Pedido'] == id_pedido]
            operacoes = []
            
            for _, row_op in ops_pedido.iterrows():
                op = Operacao(
                    id_operacao=str(row_op['ID_Operacao']).strip(),
                    recurso=str(row_op['Recurso']).strip(),
                    tempo_setup=float(row_op['Setup_H']),
                    tempo_processamento=float(row_op['Proc_H']),
                    ordem=int(row_op['Ordem'])
                )
                operacoes.append(op)
            
            # Ordenar operações
            operacoes.sort(key=lambda x: x.ordem)
            
            # Criar pedido
            data_entrega = pd.to_datetime(row_ped['Data_Entrega'])
            
            pedido = Pedido(
                id_pedido=id_pedido,
                cliente=str(row_ped['Cliente']).strip(),
                operacoes=operacoes,
                data_entrada=data_atual,
                data_entrega=data_entrega,
                prioridade=int(row_ped.get('Prioridade', 5))
            )
            pedidos.append(pedido)
        
        print(f"✓ {len(pedidos)} pedidos importados de {caminho_arquivo}")
        return pedidos
    
    @staticmethod
    def importar_recursos_csv(caminho_arquivo: str) -> Dict[str, Recurso]:
        """Importa recursos de arquivo CSV"""
        df = pd.read_csv(caminho_arquivo)
        recursos = {}
        
        for _, row in df.iterrows():
            nome = str(row['Recurso']).strip()
            capacidade = float(row['Capacidade_Horas_Dia'])
            recursos[nome] = Recurso(nome, capacidade)
        
        print(f"✓ {len(recursos)} recursos importados de {caminho_arquivo}")
        return recursos
    
    @staticmethod
    def importar_json(caminho_arquivo: str) -> Tuple[Dict[str, Recurso], List[Pedido]]:
        """
        Importa dados completos de arquivo JSON
        
        Formato esperado:
        {
          "recursos": [
            {"nome": "Torno", "capacidade_horas_dia": 16.0}
          ],
          "pedidos": [
            {
              "id_pedido": "PED-001",
              "cliente": "ABC",
              "data_entrega": "2026-02-15",
              "prioridade": 8,
              "operacoes": [
                {
                  "id_operacao": "OP-001-1",
                  "recurso": "Torno",
                  "tempo_setup": 0.5,
                  "tempo_processamento": 2.0,
                  "ordem": 1
                }
              ]
            }
          ]
        }
        """
        with open(caminho_arquivo, 'r', encoding='utf-8') as f:
            dados = json.load(f)
        
        # Importar recursos
        recursos = {}
        for rec in dados['recursos']:
            nome = rec['nome']
            recursos[nome] = Recurso(nome, rec['capacidade_horas_dia'])
        
        # Importar pedidos
        pedidos = []
        data_atual = datetime.now()
        
        for ped in dados['pedidos']:
            operacoes = []
            for op in ped['operacoes']:
                operacoes.append(Operacao(
                    id_operacao=op['id_operacao'],
                    recurso=op['recurso'],
                    tempo_setup=op['tempo_setup'],
                    tempo_processamento=op['tempo_processamento'],
                    ordem=op['ordem']
                ))
            
            operacoes.sort(key=lambda x: x.ordem)
            
            pedido = Pedido(
                id_pedido=ped['id_pedido'],
                cliente=ped['cliente'],
                operacoes=operacoes,
                data_entrada=data_atual,
                data_entrega=datetime.strptime(ped['data_entrega'], '%Y-%m-%d'),
                prioridade=ped.get('prioridade', 5)
            )
            pedidos.append(pedido)
        
        print(f"✓ {len(recursos)} recursos e {len(pedidos)} pedidos importados de {caminho_arquivo}")
        return recursos, pedidos


class RelatoriosAvancados:
    """Gera relatórios gerenciais avançados"""
    
    @staticmethod
    def relatorio_kpis(sistema: SistemaPCP_TOC) -> pd.DataFrame:
        """
        Gera KPIs principais do sistema
        """
        pedidos_ativos = [p for p in sistema.pedidos if p.status != StatusPedido.CONCLUIDO]
        
        # Calcular métricas
        total_pedidos = len(sistema.pedidos)
        pedidos_verde = sum(1 for p in pedidos_ativos 
                           if sistema.gestor_buffer.classificar_zona_buffer(p.penetracao_buffer) == "VERDE")
        pedidos_amarelo = sum(1 for p in pedidos_ativos 
                             if sistema.gestor_buffer.classificar_zona_buffer(p.penetracao_buffer) == "AMARELO")
        pedidos_vermelho = sum(1 for p in pedidos_ativos 
                              if sistema.gestor_buffer.classificar_zona_buffer(p.penetracao_buffer) == "VERMELHO")
        
        # Utilização média
        util_media = np.mean([r.calcular_utilizacao() for r in sistema.recursos.values()])
        
        # Lead time médio
        lead_times = [(p.data_entrega - p.data_entrada).days for p in sistema.pedidos]
        lead_time_medio = np.mean(lead_times) if lead_times else 0
        
        kpis = {
            'Métrica': [
                'Total de Pedidos',
                'Pedidos em Verde',
                'Pedidos em Amarelo',
                'Pedidos em Vermelho',
                'Utilização Média (%)',
                'Lead Time Médio (dias)',
                'Recurso Gargalo',
                'Total de Recursos'
            ],
            'Valor': [
                total_pedidos,
                pedidos_verde,
                pedidos_amarelo,
                pedidos_vermelho,
                f"{util_media:.1f}%",
                f"{lead_time_medio:.1f}",
                sistema.gestor_gargalo.recurso_gargalo_atual,
                len(sistema.recursos)
            ]
        }
        
        return pd.DataFrame(kpis)
    
    @staticmethod
    def relatorio_pedidos_criticos(sistema: SistemaPCP_TOC, top_n: int = 10) -> pd.DataFrame:
        """Identifica pedidos mais críticos (maior penetração de buffer)"""
        pedidos_ativos = [p for p in sistema.pedidos if p.status != StatusPedido.CONCLUIDO]
        
        # Ordenar por penetração
        pedidos_criticos = sorted(pedidos_ativos, key=lambda p: p.penetracao_buffer, reverse=True)[:top_n]
        
        dados = []
        for p in pedidos_criticos:
            zona = sistema.gestor_buffer.classificar_zona_buffer(p.penetracao_buffer)
            dias_restantes = (p.data_entrega - sistema.data_atual).days
            
            dados.append({
                'Pedido': p.id_pedido,
                'Cliente': p.cliente,
                'Penetração (%)': round(p.penetracao_buffer, 1),
                'Zona': zona,
                'Dias Restantes': dias_restantes,
                'Prioridade': p.prioridade,
                'Status': p.status.value
            })
        
        return pd.DataFrame(dados)
    
    @staticmethod
    def relatorio_capacidade_periodo(sistema: SistemaPCP_TOC, 
                                     dias: int = 30) -> pd.DataFrame:
        """Análise de capacidade para os próximos N dias"""
        dados = []
        
        for nome, recurso in sistema.recursos.items():
            capacidade_total = recurso.capacidade_horas_dia * dias
            carga_planejada = recurso.carga_planejada
            disponivel = capacidade_total - carga_planejada
            percentual_uso = (carga_planejada / capacidade_total * 100) if capacidade_total > 0 else 0
            
            dados.append({
                'Recurso': nome,
                'Capacidade Total (h)': round(capacidade_total, 1),
                'Carga Planejada (h)': round(carga_planejada, 1),
                'Disponível (h)': round(disponivel, 1),
                'Utilização (%)': round(percentual_uso, 1),
                'Status': 'GARGALO' if nome == sistema.gestor_gargalo.recurso_gargalo_atual else 'OK'
            })
        
        return pd.DataFrame(dados)
    
    @staticmethod
    def exportar_para_excel(sistema: SistemaPCP_TOC, 
                           caminho_saida: str = 'relatorio_pcp_toc.xlsx'):
        """
        Exporta todos os relatórios para um único arquivo Excel
        """
        with pd.ExcelWriter(caminho_saida, engine='openpyxl') as writer:
            # Aba 1: KPIs
            df_kpis = RelatoriosAvancados.relatorio_kpis(sistema)
            df_kpis.to_excel(writer, sheet_name='KPIs', index=False)
            
            # Aba 2: Buffers
            df_buffers = sistema.gerar_relatorio_buffer()
            df_buffers.to_excel(writer, sheet_name='Buffers', index=False)
            
            # Aba 3: Recursos
            df_recursos = sistema.gerar_relatorio_recursos()
            df_recursos.to_excel(writer, sheet_name='Recursos', index=False)
            
            # Aba 4: Pedidos Críticos
            df_criticos = RelatoriosAvancados.relatorio_pedidos_criticos(sistema)
            df_criticos.to_excel(writer, sheet_name='Pedidos_Criticos', index=False)
            
            # Aba 5: Capacidade
            df_capacidade = RelatoriosAvancados.relatorio_capacidade_periodo(sistema)
            df_capacidade.to_excel(writer, sheet_name='Capacidade_30d', index=False)
        
        print(f"✓ Relatório completo exportado para: {caminho_saida}")
        return caminho_saida


class SistemaPCP_Empresarial(SistemaPCP_TOC):
    """Extensão do sistema base para uso empresarial"""
    
    def __init__(self):
        super().__init__()
        self.importador = ImportadorDados()
        self.relatorios = RelatoriosAvancados()
    
    def carregar_dados_excel(self, caminho_arquivo: str):
        """Carrega recursos e pedidos de um arquivo Excel"""
        print(f"\n📂 Carregando dados de: {caminho_arquivo}")
        
        # Importar recursos
        recursos_importados = self.importador.importar_recursos_excel(caminho_arquivo)
        for nome, recurso in recursos_importados.items():
            self.recursos[nome] = recurso
        
        # Importar pedidos
        pedidos_importados = self.importador.importar_pedidos_excel(caminho_arquivo)
        self.pedidos.extend(pedidos_importados)
        
        print(f"✓ Dados carregados com sucesso!")
    
    def carregar_dados_json(self, caminho_arquivo: str):
        """Carrega recursos e pedidos de um arquivo JSON"""
        print(f"\n📂 Carregando dados de: {caminho_arquivo}")
        
        recursos_importados, pedidos_importados = self.importador.importar_json(caminho_arquivo)
        
        for nome, recurso in recursos_importados.items():
            self.recursos[nome] = recurso
        
        self.pedidos.extend(pedidos_importados)
        
        print(f"✓ Dados carregados com sucesso!")
    
    def gerar_relatorio_completo(self, caminho_saida: str = 'relatorio_pcp_toc.xlsx'):
        """Gera relatório Excel completo"""
        return self.relatorios.exportar_para_excel(self, caminho_saida)
    
    def executar_analise_completa(self, exportar_excel: bool = True):
        """
        Executa análise completa: planejamento + relatórios + exportação
        """
        # Executar planejamento
        self.executar_planejamento()
        
        # Exibir KPIs
        print("\n" + "=" * 80)
        print("📊 KPIs PRINCIPAIS")
        print("=" * 80)
        df_kpis = self.relatorios.relatorio_kpis(self)
        print(df_kpis.to_string(index=False))
        
        # Exibir pedidos críticos
        print("\n" + "=" * 80)
        print("⚠️  TOP 10 PEDIDOS CRÍTICOS")
        print("=" * 80)
        df_criticos = self.relatorios.relatorio_pedidos_criticos(self, top_n=10)
        if not df_criticos.empty:
            print(df_criticos.to_string(index=False))
        else:
            print("Nenhum pedido crítico no momento.")
        
        # Exportar para Excel
        if exportar_excel:
            print("\n" + "=" * 80)
            print("📁 EXPORTANDO RELATÓRIOS")
            print("=" * 80)
            arquivo = self.gerar_relatorio_completo()
            print(f"\n✓ Análise completa! Relatórios salvos em: {arquivo}")


def criar_template_excel(caminho_saida: str = 'template_dados_pcp.xlsx'):
    """
    Cria template Excel para entrada de dados
    """
    # Aba Recursos
    df_recursos = pd.DataFrame({
        'Recurso': ['Torno', 'Fresa', 'Retifica', 'Solda'],
        'Capacidade_Horas_Dia': [16.0, 8.0, 8.0, 6.0]
    })
    
    # Aba Pedidos
    df_pedidos = pd.DataFrame({
        'ID_Pedido': ['PED-001', 'PED-002', 'PED-003'],
        'Cliente': ['Cliente A', 'Cliente B', 'Cliente C'],
        'Data_Entrega': ['2026-02-15', '2026-02-10', '2026-02-20'],
        'Prioridade': [8, 10, 5]
    })
    
    # Aba Operações
    df_operacoes = pd.DataFrame({
        'ID_Pedido': ['PED-001', 'PED-001', 'PED-001', 'PED-002', 'PED-002', 'PED-003', 'PED-003'],
        'ID_Operacao': ['OP-001-1', 'OP-001-2', 'OP-001-3', 'OP-002-1', 'OP-002-2', 'OP-003-1', 'OP-003-2'],
        'Recurso': ['Torno', 'Fresa', 'Retifica', 'Torno', 'Solda', 'Fresa', 'Retifica'],
        'Setup_H': [0.5, 0.3, 0.2, 0.4, 0.5, 0.3, 0.2],
        'Proc_H': [2.0, 1.5, 1.0, 1.5, 2.5, 1.2, 0.8],
        'Ordem': [1, 2, 3, 1, 2, 1, 2]
    })
    
    # Salvar
    with pd.ExcelWriter(caminho_saida, engine='openpyxl') as writer:
        df_recursos.to_excel(writer, sheet_name='Recursos', index=False)
        df_pedidos.to_excel(writer, sheet_name='Pedidos', index=False)
        df_operacoes.to_excel(writer, sheet_name='Operacoes', index=False)
    
    print(f"✓ Template criado: {caminho_saida}")
    print("\nPreencha o template com seus dados e use:")
    print("  sistema.carregar_dados_excel('template_dados_pcp.xlsx')")
    
    return caminho_saida


if __name__ == "__main__":
    print("=" * 80)
    print("SISTEMA PCP-TOC EMPRESARIAL")
    print("Versão para uso em empresas reais")
    print("=" * 80)
    
    # Criar template de exemplo
    print("\n[1] Criando template Excel...")
    criar_template_excel()
    
    # Demonstração com template
    print("\n[2] Demonstrando importação de dados...")
    sistema = SistemaPCP_Empresarial()
    sistema.carregar_dados_excel('template_dados_pcp.xlsx')
    
    # Executar análise completa
    print("\n[3] Executando análise completa...")
    sistema.executar_analise_completa(exportar_excel=True)
    
    print("\n✅ Sistema empresarial pronto para uso!")
