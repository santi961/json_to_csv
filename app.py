#!/usr/bin/env python3
import streamlit as st
import json, os, math, re, io, zipfile
import pandas as pd
import yaml
from collections import defaultdict
from io import BytesIO

# ---------- Load Config ----------
CONFIG_PATH = 'config.yaml'

def load_config(path=CONFIG_PATH):
    if not os.path.exists(path):
        st.error(f"Config file not found: {path}")
        return {'Periods': {}, 'Placements': {}}
    with open(path) as f:
        return yaml.safe_load(f)

config = load_config()
period_map = config.get('Periods', {})
placement_map = config.get('Placements', {})

# ---------- Utility Functions ----------

def ms_to_hhmmss(total_ms):
    total_s = math.ceil(total_ms / 1000)
    if total_s == 0:
        return ''
    h, rem = divmod(total_s, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"  # HH:MM:SS


def normalize_with_map(value, mapping):
    v0 = value.strip()
    for canon, aliases in mapping.items():
        if v0.lower() == canon.lower() or any(v0.lower() == a.lower() for a in aliases):
            return canon
    return value


def normalize_period(p):
    # config-based map
    np_ = normalize_with_map(p, period_map)
    if np_ != p:
        return np_
    p0 = p.replace(' ', '').lower()
    m = re.match(r"^(\d+)(t|top)$", p0)
    if m:
        return f"{m.group(1)} Top"
    m = re.match(r"^(\d+)(b|bot|bottom)$", p0)
    if m:
        return f"{m.group(1)} Bottom"
    spaced = re.sub(r"(\d+)([A-Za-z]+)", r"\1 \2", p.replace(' ', ''))
    return spaced.title()


def normalize_placement(pl):
    return normalize_with_map(pl, placement_map)


def normalize_sponsor(name):
    base = os.path.splitext(name)[0]
    return re.sub(r"\(\d+\)$", "", base)


def sort_period_key(p):
    num, _, pos = p.partition(' ')
    return (int(num), 0 if pos.lower().startswith('t') else 1)

# ---------- Data Processing ----------

def process_data(data):
    logos = {(l['FileName'], l['GroupId']): normalize_placement(l.get('Placement', ''))
             for l in data.get('Logos', [])}
    stats, periods = {}, set()
    for shot in data.get('Shots', []):
        key = (shot['FileName'], shot['GroupId'])
        if key not in logos:
            continue
        period = normalize_period(shot['Period'])
        periods.add(period)
        ent = stats.setdefault(key, {'count': 0, 'dur_ms': 0.0, 'screen_sum': 0.0, 'periods': {}})
        ent['count'] += 1
        ent['dur_ms'] += shot.get('Duration', 0)
        ent['screen_sum'] += shot.get('ScreenPercentage', 0)
        ent['periods'][period] = ent['periods'].get(period, 0.0) + shot.get('Duration', 0)
    sponsor_stats = {}
    for (fn, gid), placement in logos.items():
        s = stats.get((fn, gid), {})
        sponsor = normalize_sponsor(fn)
        key = (sponsor, placement)
        agg = sponsor_stats.setdefault(key, {'count': 0, 'dur_ms': 0.0, 'screen_sum': 0.0, 'periods': {}})
        agg['count'] += s.get('count', 0)
        agg['dur_ms'] += s.get('dur_ms', 0.0)
        agg['screen_sum'] += s.get('screen_sum', 0.0)
        for per, ms in s.get('periods', {}).items():
            agg['periods'][per] = agg['periods'].get(per, 0.0) + ms
    return sponsor_stats, sorted(periods, key=sort_period_key)

# ---------- Builders ----------

def build_individual_df(sponsor_stats, periods):
    rows = []
    for (sponsor, placement), agg in sponsor_stats.items():
        cnt = agg['count']
        if cnt == 0:
            continue
        total_shots = math.ceil(cnt)
        row = {
            'Placement': placement,
            'Sponsor': sponsor,
            'Total Shots': total_shots,
            'Total Duration': ms_to_hhmmss(agg['dur_ms']),
            'Avg Screen %': f"{(agg['screen_sum']/cnt):.2f}%"
        }
        for per in periods:
            row[per] = ms_to_hhmmss(agg['periods'].get(per, 0))
        rows.append(row)
    cols = ['Sponsor', 'Placement', 'Total Shots', 'Total Duration', 'Avg Screen %'] + periods
    return pd.DataFrame(rows, columns=cols)


def build_aggregate_df(individual_dfs, periods):
    placement_acc = defaultdict(lambda: {'shots': [], 'durs': [], 'screens': [], 'periods': defaultdict(list)})
    def parse_ms(hms):
        if not isinstance(hms, str) or not hms:
            return 0
        h, m, s = map(int, hms.split(':'))
        return (h*3600 + m*60 + s) * 1000
    for gid, df in individual_dfs:
        for per in periods:
            if per not in df.columns:
                df[per] = ''
        grp = df.groupby('Placement').agg({
            'Total Shots': 'sum',
            'Total Duration': lambda col: sum(parse_ms(x) for x in col),
            'Avg Screen %': lambda col: sum(float(x.strip('%')) for x in col) / len(col)
        }).rename(columns={'Avg Screen %': 'AvgScreenRaw'})
        for per in periods:
            grp[per] = df.groupby('Placement')[per].apply(lambda col: sum(parse_ms(x) for x in col if isinstance(x, str)))
        for pl, row in grp.iterrows():
            acc = placement_acc[pl]
            acc['shots'].append(row['Total Shots'])
            acc['durs'].append(row['Total Duration'])
            acc['screens'].append(row['AvgScreenRaw'])
            for per in periods:
                val = row.get(per, 0)
                if pd.notna(val) and val > 0:
                    acc['periods'][per].append(val)
    rows = []
    for pl, acc in placement_acc.items():
        n = len(acc['shots'])
        avg_shots = math.ceil(sum(acc['shots']) / n) if n else 0
        avg_dur_ms = sum(acc['durs']) / n if n else 0
        avg_screen = sum(acc['screens']) / n if n else 0
        row = {
            'Placement': pl,
            'Avg Shots': avg_shots,
            'Avg Total Duration': ms_to_hhmmss(avg_dur_ms),
            'Avg Screen %': f"{avg_screen:.2f}%"
        }
        for per in periods:
            lst = acc['periods'].get(per, [])
            if lst:
                avg_p = sum(lst) / len(lst)
                row[f"Avg {per}"] = ms_to_hhmmss(avg_p)
            else:
                row[f"Avg {per}"] = '00:00:00'
        rows.append(row)
    cols = ['Placement', 'Avg Shots', 'Avg Total Duration', 'Avg Screen %'] + [f"Avg {p}" for p in periods]
    return pd.DataFrame(rows, columns=cols)

# ---------- Streamlit UI ----------

def main():
    st.title('JSON → Excel Exposure Report')
    st.markdown('Upload multiple JSONs or a ZIP of JSONs in one box, then generate.')

    uploads = st.file_uploader(
        'Upload .json files or a .zip of JSONs',
        type=['json', 'zip'],
        accept_multiple_files=True
    )
    aggregate = st.checkbox('Create aggregated xlsx')

    if st.button('Generate'):
        individual_dfs = []
        periods = set()
        game_ids = []

        def load_data(data):
            sponsor_stats, ps = process_data(data)
            periods.update(ps)
            df = build_individual_df(sponsor_stats, sorted(ps, key=sort_period_key))
            gid = data['GameInfo']['GameId'].replace(' ', '_')
            individual_dfs.append((gid, df))
            game_ids.append(gid)

        if uploads:
            for f in uploads:
                name = f.name.lower()
                if name.endswith('.json'):
                    try:
                        d = json.load(f)
                    except:
                        st.error(f"Bad JSON {f.name}")
                        continue
                    load_data(d)
                elif name.endswith('.zip'):
                    z = zipfile.ZipFile(io.BytesIO(f.read()))
                    for member in z.namelist():
                        if member.lower().endswith('.json'):
                            try:
                                d = json.loads(z.read(member))
                            except:
                                st.error(f"Bad JSON in ZIP: {member}")
                                continue
                            load_data(d)
        
        periods = sorted(periods, key=sort_period_key)
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            if aggregate:
                agg_df = build_aggregate_df(individual_dfs, periods)
                writer.book.create_sheet('Aggregate', 0)
                agg_df.to_excel(writer, sheet_name='Aggregate', index=False)
            for gid, df in individual_dfs:
                df.to_excel(writer, sheet_name=gid[:31], index=False)
        
        output.seek(0)
        fn = f"{'_'.join(game_ids)}{('_Aggregate.xlsx' if aggregate else '.xlsx')}"
        st.download_button('Download Excel', data=output, file_name=fn,
                           mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

if __name__=='__main__':
    main()
