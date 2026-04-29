from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import pandas as pd
from flask import Flask, render_template, request

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_EXCEL = BASE_DIR / 'data' / 'base.xlsx'
EXCEL_PATH = Path(os.getenv('EXCEL_PATH', DEFAULT_EXCEL))

app = Flask(__name__)

COLUMN_ALIASES = {
    'dt': 'Dt',
    'data': 'Dt',
    'hour': 'Hour',
    'hora': 'Hour',
    'nomecampanha': 'NomeCampanha',
    'campanha': 'NomeCampanha',
    'ad': 'AD',
    'ath': 'ATH',
    'mailing': 'Mailing',
    'discado': 'Discado',
    'atendidas': 'Atendidas',
    'transferencia': 'Transferencia',
    'transferidas': 'Transferencia',
    'recebidas': 'Recebidas',
    'cpc': 'Cpc',
    'acordo': 'Acordo',
    'tma_locator': 'TMA_LOCATOR',
    'tma_ath': 'TMA_ATH',
    'hitrate': 'HitRate',
    'hitrate%': 'HitRate',
    'loc': 'Loc',
    'conversao': 'Conversao',
    'spin': 'Spin',
    '%abandono': '%Abandono',
    'abandono': '%Abandono',
    'perda': 'Perda',
    '%perda': '%Perda',
    'custo': 'Custo',
    'custotelecom': 'Custo',
    'custodetelecom': 'Custo',
}

REQUIRED_COLUMNS = [
    'Dt', 'Hour', 'NomeCampanha', 'Discado', 'Atendidas', 'Transferencia', 'Recebidas', 'Cpc', 'Acordo'
]

FLOW_ORDER = [
    ('Discado', '📞'),
    ('Atendidas', '🟢'),
    ('Transferencia', '🤖'),
    ('Perda', '⚠️'),
    ('Recebidas', '🎧'),
    ('Cpc', '✅'),
    ('Acordo', '💰'),
    ('Custo', '📡'),
]

COMPARE_METRICS = [
    ('Mailing', 'Mailing', '🗂️', 'higher'),
    ('AD', 'Logados AD', '🤖', 'higher'),
    ('ATH', 'Logados ATH', '👤', 'higher'),
    ('Discado', 'Discado', '📞', 'higher'),
    ('Atendidas', 'Atendidas', '🟢', 'higher'),
    ('Transferencia', 'CPC AD / Transferidas', '✅', 'higher'),
    ('Acordo', 'Acordo', '💰', 'higher'),
    ('Custo', 'Custo Telecom', '📡', 'lower'),
    ('HitRate', 'Hit Rate', '🎯', 'higher'),
    ('TxTransferencia', 'Tx Transferência', '🔁', 'higher'),
    ('Conversao', 'Conversão Acordo', '🏁', 'higher'),
]


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    for col in df.columns:
        key = str(col).strip().lower().replace(' ', '')
        if key in COLUMN_ALIASES:
            rename_map[col] = COLUMN_ALIASES[key]
    return df.rename(columns=rename_map)


def parse_percent_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str)
        .str.replace('%', '', regex=False)
        .str.replace('.', '', regex=False)
        .str.replace(',', '.', regex=False),
        errors='coerce',
    )


def load_data() -> pd.DataFrame:
    if not EXCEL_PATH.exists():
        raise FileNotFoundError(
            f'Arquivo Excel não encontrado em: {EXCEL_PATH}. Coloque sua base em data/base.xlsx ou defina EXCEL_PATH.'
        )

    if EXCEL_PATH.suffix.lower() in {'.xlsx', '.xls'}:
        df = pd.read_excel(EXCEL_PATH)
    elif EXCEL_PATH.suffix.lower() == '.csv':
        df = pd.read_csv(EXCEL_PATH, sep=None, engine='python')
    else:
        raise ValueError('Formato de arquivo não suportado. Use .xlsx, .xls ou .csv')

    df = normalize_columns(df)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f'Colunas obrigatórias ausentes: {", ".join(missing)}')

    df['Dt'] = pd.to_datetime(df['Dt'], errors='coerce')
    df['Hour'] = pd.to_numeric(df['Hour'], errors='coerce').fillna(0).astype(int)
    df['NomeCampanha'] = df['NomeCampanha'].astype(str)

    numeric_cols = ['AD', 'ATH', 'Mailing', 'Discado', 'Atendidas', 'Transferencia', 'Perda', 'Recebidas', 'Cpc', 'Acordo', 'Spin', 'Custo']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        else:
            df[col] = 0.0

    percent_cols = ['HitRate', 'Loc', 'Conversao', '%Abandono', '%Perda']
    for col in percent_cols:
        if col in df.columns:
            df[col] = parse_percent_series(df[col])

    for tma_col in ['TMA_LOCATOR', 'TMA_ATH']:
        if tma_col in df.columns:
            df[tma_col] = df[tma_col].fillna('--').astype(str)

    df = df.dropna(subset=['Dt']).copy()
    df['DtStr'] = df['Dt'].dt.strftime('%Y-%m-%d')
    return df


def safe_pct(num: float, den: float) -> float:
    if not den:
        return 0.0
    return (num / den) * 100


def fmt_int(v: float | int) -> str:
    return f"{int(round(v)):,}".replace(',', '.')


def fmt_pct(v: float | int) -> str:
    return f"{v:,.2f}%".replace(',', 'X').replace('.', ',').replace('X', '.')


def fmt_currency(v: float | int) -> str:
    return f"R$ {v:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')


def classify_abandonment(v: float) -> str:
    if v <= 3:
        return 'verde'
    if v <= 9:
        return 'amarelo'
    return 'vermelho'


def classify_delta(delta_pct: float, preference: str) -> str:
    positive = delta_pct >= 0 if preference == 'higher' else delta_pct <= 0
    return 'positivo' if positive else 'negativo'


def calc_summary(df: pd.DataFrame, use_peak_logados: bool = False) -> Dict[str, float]:
    ad_value = (
        float(df['AD'].max()) if use_peak_logados and not df.empty
        else float(df.loc[df['AD'] > 0, 'AD'].mean()) if (df['AD'] > 0).any()
        else float(df['AD'].mean()) if not df.empty else 0.0
    )
    ath_value = (
        float(df['ATH'].max()) if use_peak_logados and not df.empty
        else float(df.loc[df['ATH'] > 0, 'ATH'].mean()) if (df['ATH'] > 0).any()
        else float(df['ATH'].mean()) if not df.empty else 0.0
    )

    totals = {
        'Mailing': float(df['Mailing'].max()) if not df.empty else 0.0,
        'AD': ad_value,
        'ATH': ath_value,
        'Discado': float(df['Discado'].sum()),
        'Atendidas': float(df['Atendidas'].sum()),
        'Transferencia': float(df['Transferencia'].sum()),
        'Recebidas': float(df['Recebidas'].sum()),
        'Cpc': float(df['Cpc'].sum()),
        'Acordo': float(df['Acordo'].sum()),
        'Perda': float(df['Perda'].sum()),
        'Custo': float(df['Custo'].sum()),
        'Spin': float(df['Spin'].mean()) if not df.empty else 0.0,
    }
    totals['PerdaPct'] = safe_pct(totals['Perda'], totals['Transferencia'])
    totals['Abandono'] = safe_pct(totals['Transferencia'] - totals['Recebidas'], totals['Transferencia'])
    totals['HitRate'] = safe_pct(totals['Atendidas'], totals['Discado'])
    totals['TxTransferencia'] = safe_pct(totals['Transferencia'], totals['Atendidas'])
    totals['TxRecebimento'] = safe_pct(totals['Recebidas'], totals['Transferencia'])
    totals['TxCpc'] = safe_pct(totals['Cpc'], totals['Recebidas'])
    totals['Conversao'] = safe_pct(totals['Acordo'], totals['Cpc'])
    return totals


def summarize_main(df: pd.DataFrame) -> Dict[str, Any]:
    totals = calc_summary(df, use_peak_logados=True)

    capacity = [
        {'label': 'AD Logados', 'icon': '🤖', 'value': fmt_int(totals['AD']), 'ratio': 'Pico no período'},
        {'label': 'ATH Logados', 'icon': '👤', 'value': fmt_int(totals['ATH']), 'ratio': 'Pico no período'},
        {'label': 'Mailing', 'icon': '🗂️', 'value': fmt_int(totals['Mailing']), 'ratio': 'Base disponível'},
    ]

    flow = []
    prior = None
    for key, icon in FLOW_ORDER:
        value = totals[key]
        if key == 'Custo':
            formatted = fmt_currency(value)
            ratio = 'Custo acumulado'
        elif key == 'Perda':
            formatted = fmt_int(value)
            ratio = fmt_pct(totals['PerdaPct'])
        else:
            formatted = fmt_int(value)
            if prior is None:
                ratio = 'Base do fluxo'
            elif key == 'Atendidas':
                ratio = fmt_pct(safe_pct(totals['Atendidas'], totals['Discado']))
            elif key == 'Transferencia':
                ratio = fmt_pct(safe_pct(totals['Transferencia'], totals['Atendidas']))
            elif key == 'Recebidas':
                ratio = fmt_pct(safe_pct(totals['Recebidas'], totals['Transferencia']))
            elif key == 'Cpc':
                ratio = fmt_pct(safe_pct(totals['Cpc'], totals['Recebidas']))
            elif key == 'Acordo':
                ratio = fmt_pct(safe_pct(totals['Acordo'], totals['Cpc']))
            else:
                ratio = ''
        flow.append({'label': {'Perda': 'Perda', 'Transferencia': 'Transferidas (CPC AD)', 'Recebidas': 'Recebidas Operador', 'Cpc': 'CPC Operador', 'Custo': 'Custo Telecom'}.get(key, key), 'icon': icon, 'value': formatted, 'ratio': ratio})
        prior = key

    base_group = df.groupby(['DtStr', 'Hour'], as_index=False).agg({
        'Mailing': 'max', 'AD': 'max', 'ATH': 'max', 'Discado': 'sum', 'Atendidas': 'sum',
        'Transferencia': 'sum', 'Perda': 'sum', 'Recebidas': 'sum', 'Cpc': 'sum', 'Acordo': 'sum', 'Custo': 'sum', 'Spin': 'mean'
    }).sort_values(['DtStr', 'Hour'])

    base_group['Hit Rate'] = base_group.apply(lambda x: safe_pct(x['Atendidas'], x['Discado']), axis=1)
    base_group['Tx Transferência'] = base_group.apply(lambda x: safe_pct(x['Transferencia'], x['Atendidas']), axis=1)
    base_group['Taxa Perda'] = base_group.apply(lambda x: safe_pct(x['Perda'], x['Transferencia']), axis=1)
    base_group['Taxa Abandono'] = base_group.apply(lambda x: safe_pct(x['Transferencia'] - x['Recebidas'], x['Transferencia']), axis=1)
    base_group['Tx Recebimento'] = base_group.apply(lambda x: safe_pct(x['Recebidas'], x['Transferencia']), axis=1)
    base_group['Tx CPC Operador'] = base_group.apply(lambda x: safe_pct(x['Cpc'], x['Recebidas']), axis=1)
    base_group['Conv. Acordo'] = base_group.apply(lambda x: safe_pct(x['Acordo'], x['Cpc']), axis=1)

    by_hour = base_group.groupby('Hour', as_index=False).agg({
        'Discado': 'sum', 'Atendidas': 'sum', 'Transferencia': 'sum', 'Perda': 'sum', 'Recebidas': 'sum', 'Cpc': 'sum', 'Acordo': 'sum',
        'Spin': 'mean'
    }).sort_values('Hour')
    by_hour['Hit Rate'] = by_hour.apply(lambda x: safe_pct(x['Atendidas'], x['Discado']), axis=1)
    by_hour['Taxa Perda'] = by_hour.apply(lambda x: safe_pct(x['Perda'], x['Transferencia']), axis=1)
    by_hour['Taxa Abandono'] = by_hour.apply(lambda x: safe_pct(x['Transferencia'] - x['Recebidas'], x['Transferencia']), axis=1)
    by_hour['Tx Recebimento'] = by_hour.apply(lambda x: safe_pct(x['Recebidas'], x['Transferencia']), axis=1)
    by_hour['Tx CPC Operador'] = by_hour.apply(lambda x: safe_pct(x['Cpc'], x['Recebidas']), axis=1)
    by_hour['Conv. Acordo'] = by_hour.apply(lambda x: safe_pct(x['Acordo'], x['Cpc']), axis=1)
    by_hour['Tx Transferência'] = by_hour.apply(lambda x: safe_pct(x['Transferencia'], x['Atendidas']), axis=1)

    chart_labels = [f"{int(h):02d}:00" for h in by_hour['Hour'].tolist()]
    chart_series = {
        'discado': by_hour['Discado'].round(0).astype(int).tolist(),
        'atendidas': by_hour['Atendidas'].round(0).astype(int).tolist(),
        'transferidas': by_hour['Transferencia'].round(0).astype(int).tolist(),
        'perda': by_hour['Perda'].round(0).astype(int).tolist(),
        'recebidas': by_hour['Recebidas'].round(0).astype(int).tolist(),
        'cpc': by_hour['Cpc'].round(0).astype(int).tolist(),
        'acordo': by_hour['Acordo'].round(0).astype(int).tolist(),
        'spin': by_hour['Spin'].round(2).tolist(),
        'hit_rate': by_hour['Hit Rate'].round(2).tolist(),
        'perda_pct': by_hour['Taxa Perda'].round(2).tolist(),
        'abandono': by_hour['Taxa Abandono'].round(2).tolist(),
        'tx_recebimento': by_hour['Tx Recebimento'].round(2).tolist(),
        'tx_cpc': by_hour['Tx CPC Operador'].round(2).tolist(),
        'conversao': by_hour['Conv. Acordo'].round(2).tolist(),
    }

    hourly_table = []
    for _, row in base_group.iterrows():
        hourly_table.append({
            'Data': row['DtStr'],
            'Hour': f"{int(row['Hour']):02d}:00",
            'Mailing': fmt_int(row['Mailing']),
            'AD': fmt_int(row['AD']),
            'ATH': fmt_int(row['ATH']),
            'Discado': fmt_int(row['Discado']),
            'Atendidas': fmt_int(row['Atendidas']),
            'Transferencia': fmt_int(row['Transferencia']),
            'Perda': fmt_int(row['Perda']),
            'Recebidas': fmt_int(row['Recebidas']),
            'Cpc': fmt_int(row['Cpc']),
            'Acordo': fmt_int(row['Acordo']),
            'Custo': fmt_currency(row['Custo']),
            'Hit Rate': fmt_pct(row['Hit Rate']),
            'Tx Transferência': fmt_pct(row['Tx Transferência']),
            'Taxa Perda': fmt_pct(row['Taxa Perda']),
            'Taxa Abandono': fmt_pct(row['Taxa Abandono']),
            'Tx Recebimento': fmt_pct(row['Tx Recebimento']),
            'Tx CPC Operador': fmt_pct(row['Tx CPC Operador']),
            'Conv. Acordo': fmt_pct(row['Conv. Acordo']),
        })

    extras = {
        'Taxa Atendimento': fmt_pct(totals['HitRate']),
        'Taxa Transferência': fmt_pct(totals['TxTransferencia']),
        'Taxa Recebimento': fmt_pct(totals['TxRecebimento']),
        'Taxa CPC Operador': fmt_pct(totals['TxCpc']),
        'Conversão Acordo': fmt_pct(totals['Conversao']),
    }

    metric_charts = [
        {'id': 'chartDiscado', 'title': 'Discado x Spin', 'bar_label': 'Discado', 'line_label': 'Spin', 'bar_key': 'discado', 'line_key': 'spin'},
        {'id': 'chartAtendidas', 'title': 'Atendidas x Hit Rate', 'bar_label': 'Atendidas', 'line_label': 'Hit Rate', 'bar_key': 'atendidas', 'line_key': 'hit_rate'},
        {'id': 'chartTransferidas', 'title': 'Transferidas x Taxa de Abandono', 'bar_label': 'Transferidas', 'line_label': 'Taxa de Abandono', 'bar_key': 'transferidas', 'line_key': 'abandono'},
        {'id': 'chartRecebidas', 'title': 'Recebidas x Taxa de Recebimento', 'bar_label': 'Recebidas', 'line_label': 'Taxa de Recebimento', 'bar_key': 'recebidas', 'line_key': 'tx_recebimento'},
        {'id': 'chartCpc', 'title': 'CPC x Taxa de CPC Operador', 'bar_label': 'CPC', 'line_label': 'Taxa de CPC Operador', 'bar_key': 'cpc', 'line_key': 'tx_cpc'},
        {'id': 'chartAcordo', 'title': 'Acordo x Taxa de Conversão', 'bar_label': 'Acordo', 'line_label': 'Taxa de Conversão', 'bar_key': 'acordo', 'line_key': 'conversao'},
    ]

    return {
        'capacity': capacity,
        'flow': flow,
        'extras': extras,
        'perda': {'label': 'Taxa Perda', 'value': fmt_pct(totals['PerdaPct']), 'class': classify_abandonment(totals['PerdaPct'])},
        'abandono': {'label': 'Taxa Abandono', 'value': fmt_pct(totals['Abandono']), 'class': classify_abandonment(totals['Abandono'])},
        'chart_labels': chart_labels,
        'chart_series': chart_series,
        'metric_charts': metric_charts,
        'hourly_table': hourly_table,
    }


def build_compare_card(label: str, icon: str, preference: str, a_val: float, b_val: float, kind: str = 'number') -> Dict[str, Any]:
    delta_abs = a_val - b_val
    delta_pct = 0.0 if b_val == 0 else ((a_val / b_val) - 1) * 100
    css = classify_delta(delta_pct, preference)

    if kind == 'currency':
        fa, fb, dabs = fmt_currency(a_val), fmt_currency(b_val), fmt_currency(delta_abs)
    elif kind == 'percent':
        fa, fb, dabs = fmt_pct(a_val), fmt_pct(b_val), fmt_pct(delta_abs)
    else:
        fa, fb, dabs = fmt_int(a_val), fmt_int(b_val), fmt_int(delta_abs)

    return {
        'label': label,
        'icon': icon,
        'a': fa,
        'b': fb,
        'delta_pct': fmt_pct(delta_pct),
        'delta_abs': dabs,
        'class': css,
    }


def summarize_comparison(df_a: pd.DataFrame, df_b: pd.DataFrame, campaign_a: str, campaign_b: str) -> Dict[str, Any]:
    sum_a = calc_summary(df_a, use_peak_logados=True)
    sum_b = calc_summary(df_b, use_peak_logados=True)

    # Regra do cenário comparativo:
    # a campanha B representa a operação ativa, então a conversão deve ser Acordo / Transferidas.
    sum_b['Conversao'] = safe_pct(sum_b['Acordo'], sum_b['Transferencia'])

    cards = []
    for key, label, icon, preference in COMPARE_METRICS:
        kind = 'number'
        if key in {'Abandono', 'HitRate', 'TxTransferencia', 'TxRecebimento', 'TxCpc', 'Conversao'}:
            kind = 'percent'
        elif key == 'Custo':
            kind = 'currency'
        cards.append(build_compare_card(label, icon, preference, sum_a.get(key, 0.0), sum_b.get(key, 0.0), kind))

    funil_a = [
        {'label': 'Mailing', 'value': fmt_int(sum_a['Mailing']), 'hint': 'Base consolidada', 'icon': '🗂️'},
        {'label': 'Discado', 'value': fmt_int(sum_a['Discado']), 'hint': f"Spin {sum_a['Spin']:.2f}".replace('.', ','), 'icon': '📞'},
        {'label': 'Atendidas', 'value': fmt_int(sum_a['Atendidas']), 'hint': f"Hit {fmt_pct(sum_a['HitRate'])}", 'icon': '🟢'},
        {'label': 'CPC AD', 'value': fmt_int(sum_a['Transferencia']), 'hint': f"Tx {fmt_pct(sum_a['TxTransferencia'])}", 'icon': '🤖'},
        {'label': 'Acordo', 'value': fmt_int(sum_a['Acordo']), 'hint': f"Conv {fmt_pct(sum_a['Conversao'])}", 'icon': '💰'},
    ]
    funil_b = [
        {'label': 'Mailing', 'value': fmt_int(sum_b['Mailing']), 'hint': 'Base consolidada', 'icon': '🗂️'},
        {'label': 'Discado', 'value': fmt_int(sum_b['Discado']), 'hint': f"Spin {sum_b['Spin']:.2f}".replace('.', ','), 'icon': '📞'},
        {'label': 'Atendidas', 'value': fmt_int(sum_b['Atendidas']), 'hint': f"Hit {fmt_pct(sum_b['HitRate'])}", 'icon': '🟢'},
        {'label': 'CPC AD', 'value': fmt_int(sum_b['Transferencia']), 'hint': f"Tx {fmt_pct(sum_b['TxTransferencia'])}", 'icon': '🤖'},
        {'label': 'Acordo', 'value': fmt_int(sum_b['Acordo']), 'hint': f"Conv {fmt_pct(sum_b['Conversao'])}", 'icon': '💰'},
    ]

    insight = None
    if sum_a['Acordo'] > sum_b['Acordo'] and sum_a['Custo'] > sum_b['Custo']:
        insight = f'{campaign_a} gera mais acordos no período, porém com custo telecom acima de {campaign_b}. Vale equilibrar eficiência e custo por conversão.'
    elif sum_a['Acordo'] > sum_b['Acordo'] and sum_a['Custo'] <= sum_b['Custo']:
        insight = f'{campaign_a} combina melhor resultado e disciplina de custo no período filtrado, com mais acordos e custo controlado frente a {campaign_b}.'
    elif sum_b['Acordo'] > sum_a['Acordo'] and sum_b['Custo'] <= sum_a['Custo']:
        insight = f'{campaign_b} apresenta o melhor equilíbrio entre conversão e custo telecom no período analisado.'
    elif sum_a['HitRate'] > sum_b['HitRate'] and sum_a['TxTransferencia'] > sum_b['TxTransferencia']:
        insight = f'{campaign_a} está mais eficiente no topo do funil, com melhor conexão e melhor passagem para a etapa seguinte.'
    elif sum_b['HitRate'] > sum_a['HitRate'] and sum_b['TxTransferencia'] > sum_a['TxTransferencia']:
        insight = f'{campaign_b} ganha tração no topo do funil e merece atenção como referência operacional para o período filtrado.'
    else:
        insight = f'As campanhas mostram comportamentos diferentes. O melhor recorte para decisão está no equilíbrio entre acordos, custo telecom e taxa de transferência.'

    return {
        'campaign_a': campaign_a,
        'campaign_b': campaign_b,
        'funil_a': funil_a,
        'funil_b': funil_b,
        'cards': cards,
        'insight': insight,
    }


def apply_main_filters(df: pd.DataFrame, campaign: str, date: str) -> pd.DataFrame:
    out = df.copy()
    if campaign != 'Todos':
        out = out[out['NomeCampanha'] == campaign]
    if date != 'Todos':
        out = out[out['DtStr'] == date]
    return out


def apply_range_filters(
    df: pd.DataFrame,
    campaign: str,
    start_date: str,
    end_date: str,
    start_hour: str = '',
    end_hour: str = '',
) -> pd.DataFrame:
    out = df.copy()
    if campaign != 'Todos':
        out = out[out['NomeCampanha'] == campaign]
    if start_date:
        out = out[out['DtStr'] >= start_date]
    if end_date:
        out = out[out['DtStr'] <= end_date]

    # Filtro único de horário para a visão comparativa.
    # Quando preenchido, ele afeta as campanhas A e B ao mesmo tempo.
    if start_hour != '':
        out = out[out['Hour'] >= int(start_hour)]
    if end_hour != '':
        out = out[out['Hour'] <= int(end_hour)]
    return out


# ===== Aba de Tabulacao =====
TAB_COLUMN_ALIASES = {
    'data': 'data',
    'dt': 'data',
    'hora': 'Hora',
    'hour': 'Hora',
    'nomecampanha': 'NomeCampanha',
    'campanha': 'NomeCampanha',
    'origem_tabulacao': 'Origem_Tabulacao',
    'origem_tabulação': 'Origem_Tabulacao',
    'origemtabulacao': 'Origem_Tabulacao',
    'origemtabulação': 'Origem_Tabulacao',
    'tipo': 'Origem_Tabulacao',
    'tabulacao': 'Tabulacao',
    'tabulação': 'Tabulacao',
    'classificacao': 'Classificacao',
    'classificação': 'Classificacao',
    'class_loc': 'Class_Loc',
    'classloc': 'Class_Loc',
    'class_loca': 'Class_Loc',
    'class_rec': 'Class_Rec',
    'classrec': 'Class_Rec',
    'quantidade': 'Quantidade',
    'qtd': 'Quantidade',
    'tempo_total_tabulacao': 'Tempo_Total_Tabulacao',
    'tempo_total_tabulação': 'Tempo_Total_Tabulacao',
    'tempototaltabulacao': 'Tempo_Total_Tabulacao',
    'tempototaltabulação': 'Tempo_Total_Tabulacao',
    'tempo_total': 'Tempo_Total_Tabulacao',
    'tma': 'TMA',
    'tma_loc': 'TMA_LOC',
    'tmaloc': 'TMA_LOC',
    'tma_locator': 'TMA_LOC',
    'tma_rec': 'TMA_REC',
    'tmarec': 'TMA_REC',
}


def clean_key(value: Any) -> str:
    return str(value).strip().lower().replace(' ', '').replace('-', '').replace('_', '')


def normalize_tabulacao_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    tempo_seen = 0
    for col in df.columns:
        raw = str(col).strip()
        key = raw.lower().replace(' ', '').replace('-', '').replace('_', '')
        key_alias = raw.strip().lower().replace(' ', '_')

        if key.startswith('tempota'):
            tempo_seen += 1
            rename_map[col] = 'Tempo_LOC' if tempo_seen == 1 else 'Tempo_REC'
        elif key_alias in TAB_COLUMN_ALIASES:
            rename_map[col] = TAB_COLUMN_ALIASES[key_alias]
        elif key in TAB_COLUMN_ALIASES:
            rename_map[col] = TAB_COLUMN_ALIASES[key]
        elif key.endswith('horas'):
            digits = ''.join(ch for ch in key if ch.isdigit())
            if digits:
                rename_map[col] = f'{int(digits)}horas'
    return df.rename(columns=rename_map)


def time_to_seconds(value: Any) -> float:
    if pd.isna(value):
        return 0.0
    if hasattr(value, 'hour') and hasattr(value, 'minute') and hasattr(value, 'second'):
        return float(value.hour * 3600 + value.minute * 60 + value.second)
    text = str(value).strip()
    if text in {'', '--', '::', 'nan', 'NaT'}:
        return 0.0
    try:
        td = pd.to_timedelta(text)
        return float(td.total_seconds())
    except Exception:
        parts = text.split(':')
        if len(parts) == 3:
            try:
                return float(int(parts[0]) * 3600 + int(parts[1]) * 60 + int(float(parts[2])))
            except Exception:
                return 0.0
    return 0.0


def fmt_time(seconds: float) -> str:
    seconds = int(round(seconds or 0))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f'{h:02d}:{m:02d}:{s:02d}'


def weighted_tma_seconds(df: pd.DataFrame, tma_col: str = 'TMA_Seg') -> float:
    if df.empty or 'Quantidade' not in df.columns:
        return 0.0
    weight = pd.to_numeric(df['Quantidade'], errors='coerce').fillna(0)
    tma = pd.to_numeric(df[tma_col], errors='coerce').fillna(0)
    den = float(weight.sum())
    if den <= 0:
        return float(tma.mean()) if len(tma) else 0.0
    return float((tma * weight).sum() / den)


def load_tabulacao_data() -> pd.DataFrame:
    """Carrega a aba de tabulação.

    Suporta dois layouts:
    1) Novo layout linha a linha:
       data, Hora, NomeCampanha, Origem_Tabulação, Tabulacao, Classificacao,
       Quantidade, Tempo_Total_Tabulação, TMA.
    2) Layout antigo com Class_Loc/Class_Rec e colunas 8horas, 9horas...
    """
    if not EXCEL_PATH.exists():
        raise FileNotFoundError(f'Arquivo Excel nao encontrado em: {EXCEL_PATH}.')
    if EXCEL_PATH.suffix.lower() not in {'.xlsx', '.xls'}:
        raise ValueError('A aba de tabulacao precisa estar em um Excel com sheets.')

    sheet_env = os.getenv('TABULACAO_SHEET', '').strip()
    if sheet_env:
        df = pd.read_excel(EXCEL_PATH, sheet_name=sheet_env)
    else:
        xls = pd.ExcelFile(EXCEL_PATH)
        if len(xls.sheet_names) < 2:
            raise ValueError('Crie uma segunda aba no Excel para a base de tabulacao ou defina TABULACAO_SHEET.')
        df = pd.read_excel(EXCEL_PATH, sheet_name=xls.sheet_names[1])

    df = normalize_tabulacao_columns(df)

    required = ['data', 'NomeCampanha', 'Tabulacao', 'Quantidade']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f'Colunas obrigatorias da aba de tabulacao ausentes: {", ".join(missing)}')

    df['data'] = pd.to_datetime(df['data'], errors='coerce')
    df = df.dropna(subset=['data']).copy()
    df['DataStr'] = df['data'].dt.strftime('%Y-%m-%d')
    df['NomeCampanha'] = df['NomeCampanha'].astype(str)
    df['Tabulacao'] = df['Tabulacao'].fillna('Sem tabulacao').astype(str)
    df['Quantidade'] = pd.to_numeric(df['Quantidade'], errors='coerce').fillna(0)

    # Novo layout: usa Origem_Tabulação para separar Locator x Receptivo.
    if 'Origem_Tabulacao' in df.columns:
        df['Tipo'] = df['Origem_Tabulacao'].fillna('').astype(str).str.strip()
        df['Tipo'] = df['Tipo'].apply(lambda x: 'Locator' if 'locator' in x.lower() else ('Receptivo' if 'receptivo' in x.lower() else x))
        df.loc[~df['Tipo'].isin(['Locator', 'Receptivo']), 'Tipo'] = df.loc[~df['Tipo'].isin(['Locator', 'Receptivo']), 'NomeCampanha'].apply(
            lambda x: 'Locator' if 'locator' in str(x).lower() else 'Receptivo'
        )
    else:
        # Layout antigo: fallback por nome da campanha.
        df['Tipo'] = df['NomeCampanha'].apply(lambda x: 'Locator' if 'locator' in str(x).lower() else 'Receptivo')

    if 'Classificacao' in df.columns:
        df['Classificacao'] = df['Classificacao'].fillna('Sem classificacao').astype(str)
    else:
        df['Class_Loc'] = df['Class_Loc'].fillna('Sem classificacao').astype(str) if 'Class_Loc' in df.columns else 'Sem classificacao'
        df['Class_Rec'] = df['Class_Rec'].fillna('Sem classificacao').astype(str) if 'Class_Rec' in df.columns else 'Sem classificacao'
        df['Classificacao'] = df.apply(lambda r: r['Class_Loc'] if r['Tipo'] == 'Locator' else r['Class_Rec'], axis=1)

    if 'Hora' in df.columns:
        df['Hora'] = pd.to_numeric(df['Hora'], errors='coerce')
        df = df.dropna(subset=['Hora']).copy()
        df['Hora'] = df['Hora'].astype(int)

    # Novo layout: TMA único por linha.
    if 'TMA' in df.columns:
        df['TMA_Seg'] = df['TMA'].apply(time_to_seconds)
    else:
        for col in ['TMA_LOC', 'TMA_REC', 'Tempo_LOC', 'Tempo_REC']:
            if col not in df.columns:
                df[col] = '00:00:00'
            df[col + '_Seg'] = df[col].apply(time_to_seconds)
        df['TMA_Seg'] = df.apply(lambda r: r['TMA_LOC_Seg'] if r['Tipo'] == 'Locator' else r['TMA_REC_Seg'], axis=1)

    if 'Tempo_Total_Tabulacao' in df.columns:
        df['Tempo_Total_Seg'] = df['Tempo_Total_Tabulacao'].apply(time_to_seconds)
    else:
        df['Tempo_Total_Seg'] = df['TMA_Seg'] * df['Quantidade']

    # Layout antigo com colunas 8horas, 9horas...
    hour_cols = []
    for col in df.columns:
        c = str(col).lower()
        if c.endswith('horas') and ''.join(ch for ch in c if ch.isdigit()):
            hour_cols.append(col)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    df.attrs['hour_cols'] = sorted(hour_cols, key=lambda x: int(''.join(ch for ch in str(x) if ch.isdigit())))
    return df


def apply_tabulacao_filters(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """Na aba de Tabulação, mantemos apenas filtro de data.
    Locator e Receptivo sempre aparecem lado a lado.
    """
    out = df.copy()
    if date != 'Todos':
        out = out[out['DataStr'] == date]
    return out


def tab_group_payload(df: pd.DataFrame, tipo: str, top_n: int = 12) -> Dict[str, Any]:
    part = df[df['Tipo'] == tipo].copy()
    if part.empty:
        return {'labels': [], 'volume': [], 'tma': [], 'tma_fmt': []}
    grouped = part.groupby('Tabulacao', as_index=False).apply(
        lambda g: pd.Series({'Quantidade': g['Quantidade'].sum(), 'TMA_Seg': weighted_tma_seconds(g)})
    ).reset_index(drop=True)
    grouped = grouped.sort_values('Quantidade', ascending=False).head(top_n)
    return {
        'labels': grouped['Tabulacao'].tolist(),
        'volume': grouped['Quantidade'].round(0).astype(int).tolist(),
        'tma': grouped['TMA_Seg'].round(0).astype(int).tolist(),
        'tma_fmt': [fmt_time(v) for v in grouped['TMA_Seg'].tolist()],
    }


def class_payload(df: pd.DataFrame) -> Dict[str, Any]:
    rows = []
    labels = sorted([x for x in df['Classificacao'].dropna().unique().tolist() if str(x).strip()])
    for cls in labels:
        loc = df[(df['Tipo'] == 'Locator') & (df['Classificacao'] == cls)]
        rec = df[(df['Tipo'] == 'Receptivo') & (df['Classificacao'] == cls)]
        loc_tma = weighted_tma_seconds(loc)
        rec_tma = weighted_tma_seconds(rec)
        rows.append({
            'Classificacao': cls,
            'Volume Locator': fmt_int(loc['Quantidade'].sum()),
            'TMA Locator': fmt_time(loc_tma),
            'Volume Receptivo': fmt_int(rec['Quantidade'].sum()),
            'TMA Receptivo': fmt_time(rec_tma),
            'loc_tma_sec': int(round(loc_tma)),
            'rec_tma_sec': int(round(rec_tma)),
        })
    rows = sorted(rows, key=lambda r: (r['loc_tma_sec'] + r['rec_tma_sec']), reverse=True)
    return {
        'labels': [r['Classificacao'] for r in rows],
        'locator_tma': [r['loc_tma_sec'] for r in rows],
        'receptivo_tma': [r['rec_tma_sec'] for r in rows],
        'rows': rows,
    }


def hour_columns(df: pd.DataFrame) -> list[str]:
    cols = []
    for col in df.columns:
        c = str(col).lower()
        if c.endswith('horas') and ''.join(ch for ch in c if ch.isdigit()):
            cols.append(col)
    return sorted(cols, key=lambda x: int(''.join(ch for ch in str(x) if ch.isdigit())))


def hourly_tma_payload(df: pd.DataFrame, selected_class: str) -> Dict[str, Any]:
    work = df.copy()
    if selected_class and selected_class != 'Todos':
        work = work[work['Classificacao'] == selected_class]

    # Novo layout: já vem uma coluna Hora linha a linha.
    if 'Hora' in work.columns:
        all_hours = sorted([int(h) for h in work['Hora'].dropna().unique().tolist()])
        labels = [f'{h:02d}:00' for h in all_hours]

        def series_for(tipo: str) -> list[int]:
            part = work[work['Tipo'] == tipo]
            values = []
            for h in all_hours:
                ph = part[part['Hora'] == h]
                values.append(int(round(weighted_tma_seconds(ph))) if not ph.empty else 0)
            return values

        return {
            'labels': labels,
            'locator': series_for('Locator'),
            'receptivo': series_for('Receptivo'),
            'selected_class': selected_class,
        }

    # Layout antigo: usa colunas 8horas, 9horas...
    hcols = hour_columns(work)
    labels = [f"{int(''.join(ch for ch in str(c) if ch.isdigit())):02d}:00" for c in hcols]

    def series_for(tipo: str) -> list[int]:
        part = work[work['Tipo'] == tipo]
        values = []
        for col in hcols:
            if part.empty:
                values.append(0)
                continue
            vol = pd.to_numeric(part[col], errors='coerce').fillna(0)
            tma = pd.to_numeric(part['TMA_Seg'], errors='coerce').fillna(0)
            den = float(vol.sum())
            values.append(int(round(float((vol * tma).sum() / den))) if den > 0 else 0)
        return values

    return {
        'labels': labels,
        'locator': series_for('Locator'),
        'receptivo': series_for('Receptivo'),
        'selected_class': selected_class,
    }


def weighted_tma_operacional(df: pd.DataFrame, tipo: str) -> float:
    """TMA consolidado dos cards principais considerando apenas Contato e CPC.

    Isso evita que grandes volumes de Discado com TMA 00:00:00 derrubem o TMA ponderado do Locator.
    """
    part = df[df['Tipo'] == tipo].copy()
    if part.empty:
        return 0.0
    cls = part['Classificacao'].astype(str).str.strip().str.lower()
    part = part[cls.isin(['contato', 'cpc'])]
    part = part[pd.to_numeric(part['TMA_Seg'], errors='coerce').fillna(0) > 0]
    return weighted_tma_seconds(part)


def classification_card_payload(df: pd.DataFrame, tipo: str, classificacoes: list[str]) -> list[Dict[str, Any]]:
    """Cards de classificação exibem somente o TMA consolidado de cada etapa."""
    part = df[df['Tipo'] == tipo]
    cards = []
    icons = {'Contato': '🤝', 'Cpc': '✅', 'CPC': '✅', 'Acordo': '💰'}
    for cls in classificacoes:
        cls_part = part[part['Classificacao'].astype(str).str.lower() == cls.lower()]
        tma = weighted_tma_seconds(cls_part)
        cards.append({
            'label': cls.upper() if cls.lower() == 'cpc' else cls,
            'icon': icons.get(cls, '📌'),
            'value': fmt_time(tma),
            'hint': 'TMA consolidado',
        })
    return cards


def summarize_tabulacao(df: pd.DataFrame, selected_class: str = 'Todos', locator_class: str = 'Todos', receptivo_class: str = 'Todos') -> Dict[str, Any]:
    loc = df[df['Tipo'] == 'Locator']
    rec = df[df['Tipo'] == 'Receptivo']
    total_qtd = float(df['Quantidade'].sum())

    locator_cards = [
        {'label': 'TMA Locator', 'icon': '⏱️', 'value': fmt_time(weighted_tma_operacional(df, 'Locator')), 'hint': 'Contato + CPC'},
    ]
    receptivo_cards = [
        {'label': 'TMA Receptivo', 'icon': '🎧', 'value': fmt_time(weighted_tma_operacional(df, 'Receptivo')), 'hint': 'Contato + CPC'},
    ]

    ranking = df.groupby(['Tipo', 'Tabulacao', 'Classificacao'], as_index=False).apply(
        lambda g: pd.Series({'Quantidade': g['Quantidade'].sum(), 'TMA_Seg': weighted_tma_seconds(g)})
    ).reset_index(drop=True).sort_values('Quantidade', ascending=False).head(20)
    ranking_rows = [{
        'Tipo': r['Tipo'],
        'Tabulacao': r['Tabulacao'],
        'Classificacao': r['Classificacao'],
        'Quantidade': fmt_int(r['Quantidade']),
        'TMA': fmt_time(r['TMA_Seg']),
    } for _, r in ranking.iterrows()]

    return {
        'locator_cards': locator_cards,
        'receptivo_cards': receptivo_cards,
        'locator_class_cards': classification_card_payload(df, 'Locator', ['Contato', 'Cpc']),
        'receptivo_class_cards': classification_card_payload(df, 'Receptivo', ['Contato', 'Cpc', 'Acordo']),
        'locator': tab_group_payload(df if locator_class == 'Todos' else df[(df['Tipo'] != 'Locator') | (df['Classificacao'] == locator_class)], 'Locator'),
        'receptivo': tab_group_payload(df if receptivo_class == 'Todos' else df[(df['Tipo'] != 'Receptivo') | (df['Classificacao'] == receptivo_class)], 'Receptivo'),
        'classes': class_payload(df),
        'hourly_tma': hourly_tma_payload(df, selected_class),
        'ranking_rows': ranking_rows,
    }


@app.route('/tabulacao')
def tabulacao() -> str:
    error = None
    context: Dict[str, Any] = {
        'page': 'tabulacao',
        'dates': [],
        'class_options': [],
        'selected_date': 'Todos',
        'selected_class': 'Todos',
        'selected_locator_class': 'Todos',
        'selected_receptivo_class': 'Todos',
        'locator_class_options': [],
        'receptivo_class_options': [],
        'summary': None,
    }
    try:
        df = load_tabulacao_data()
        dates = sorted(df['DataStr'].dropna().unique().tolist(), reverse=True)
        class_options = sorted([x for x in df['Classificacao'].dropna().unique().tolist() if str(x).strip()])
        locator_class_options = sorted([x for x in df.loc[df['Tipo'] == 'Locator', 'Classificacao'].dropna().unique().tolist() if str(x).strip()])
        receptivo_class_options = sorted([x for x in df.loc[df['Tipo'] == 'Receptivo', 'Classificacao'].dropna().unique().tolist() if str(x).strip()])

        selected_date = request.args.get('date', dates[0] if dates else 'Todos')
        selected_class = request.args.get('classificacao', 'Todos')
        selected_locator_class = request.args.get('locator_class', 'Todos')
        selected_receptivo_class = request.args.get('receptivo_class', 'Todos')

        filtered = apply_tabulacao_filters(df, selected_date)
        context.update({
            'dates': dates,
            'class_options': class_options,
            'locator_class_options': locator_class_options,
            'receptivo_class_options': receptivo_class_options,
            'selected_date': selected_date,
            'selected_class': selected_class,
            'selected_locator_class': selected_locator_class,
            'selected_receptivo_class': selected_receptivo_class,
            'summary': summarize_tabulacao(filtered, selected_class, selected_locator_class, selected_receptivo_class) if not filtered.empty else None,
        })
    except Exception as exc:
        error = str(exc)
    return render_template('tabulacao.html', error=error, **context)

@app.route('/')
def index() -> str:
    error = None
    context: Dict[str, Any] = {
        'page': 'dashboard',
        'campaigns': [],
        'dates': [],
        'selected_campaign': 'Todos',
        'selected_date': 'Todos',
        'summary': None,
    }
    try:
        df = load_data()
        campaigns = sorted(df['NomeCampanha'].dropna().unique().tolist())
        dates = sorted(df['DtStr'].dropna().unique().tolist(), reverse=True)
        selected_campaign = request.args.get('campaign', 'Todos')
        selected_date = request.args.get('date', 'Todos')
        filtered = apply_main_filters(df, selected_campaign, selected_date)
        context.update({
            'campaigns': campaigns,
            'dates': dates,
            'selected_campaign': selected_campaign,
            'selected_date': selected_date,
            'summary': summarize_main(filtered) if not filtered.empty else None,
        })
    except Exception as exc:
        error = str(exc)
    return render_template('index.html', error=error, **context)


@app.route('/comparativo')
def comparativo() -> str:
    error = None
    context: Dict[str, Any] = {
        'page': 'comparativo',
        'campaigns': [],
        'comparison': None,
        'campaign_a': 'Todos',
        'campaign_b': 'Todos',
        'start_date': '',
        'end_date': '',
        'start_hour': '',
        'end_hour': '',
        'hour_options': list(range(24)),
    }
    try:
        df = load_data()
        campaigns = sorted(df['NomeCampanha'].dropna().unique().tolist())
        dates = sorted(df['DtStr'].dropna().unique().tolist())
        default_start = dates[0] if dates else ''
        default_end = dates[-1] if dates else ''
        campaign_a = request.args.get('campaign_a', campaigns[0] if campaigns else 'Todos')
        campaign_b = request.args.get('campaign_b', campaigns[1] if len(campaigns) > 1 else (campaigns[0] if campaigns else 'Todos'))
        start_date = request.args.get('start_date', default_start)
        end_date = request.args.get('end_date', default_end)
        start_hour = request.args.get('start_hour', '')
        end_hour = request.args.get('end_hour', '')

        df_a = apply_range_filters(df, campaign_a, start_date, end_date, start_hour, end_hour)
        df_b = apply_range_filters(df, campaign_b, start_date, end_date, start_hour, end_hour)

        comparison = None
        if not df_a.empty or not df_b.empty:
            comparison = summarize_comparison(df_a, df_b, campaign_a, campaign_b)

        context.update({
            'campaigns': campaigns,
            'campaign_a': campaign_a,
            'campaign_b': campaign_b,
            'start_date': start_date,
            'end_date': end_date,
            'start_hour': start_hour,
            'end_hour': end_hour,
            'hour_options': list(range(24)),
            'comparison': comparison,
        })
    except Exception as exc:
        error = str(exc)
    return render_template('comparativo.html', error=error, **context)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
