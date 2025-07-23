# database.py
import sqlite3

DB_NAME = 'spielberichte.db'

def init_db():
    """Initialisiert die Datenbank und erstellt die notwendigen Tabellen."""
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS spiele (spielnummer TEXT PRIMARY KEY, spielklasse TEXT, spieldatum TEXT, heimmannschaft TEXT, gastmannschaft TEXT, endstand TEXT, halbzeitstand TEXT)')
    cursor.execute('CREATE TABLE IF NOT EXISTS spieler (id INTEGER PRIMARY KEY AUTOINCREMENT, spielnummer TEXT NOT NULL, mannschaftsname TEXT NOT NULL, trikotnummer TEXT, name TEXT, jahrgang TEXT, tore TEXT, sieben_meter_tore TEXT, sieben_meter_versuche TEXT, verwarnung TEXT, hinausstellung_1 TEXT, hinausstellung_2 TEXT, hinausstellung_3 TEXT, disqualifikation TEXT, FOREIGN KEY (spielnummer) REFERENCES spiele (spielnummer))')
    cursor.execute('CREATE TABLE IF NOT EXISTS spieler_aktionen (id INTEGER PRIMARY KEY AUTOINCREMENT, spielnummer TEXT NOT NULL, trikotnummer TEXT NOT NULL, mannschaftsname TEXT NOT NULL, spielzeit TEXT, aktionstyp TEXT, spielstand TEXT, FOREIGN KEY (spielnummer) REFERENCES spiele (spielnummer))')
    cursor.execute('CREATE TABLE IF NOT EXISTS mannschafts_aktionen (id INTEGER PRIMARY KEY AUTOINCREMENT, spielnummer TEXT NOT NULL, mannschaftsname TEXT NOT NULL, spielzeit TEXT, aktionstyp TEXT, spielstand TEXT, FOREIGN KEY (spielnummer) REFERENCES spiele (spielnummer))')
    conn.commit()
    conn.close()
    print("Datenbank initialisiert.")

def insert_spielbericht_data(data):
    """Fügt die Daten eines kompletten Spielberichts in die Datenbank ein."""
    spielnummer = data['spiel_info']['spielnummer']
    info = data['spiel_info']
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cursor = conn.cursor()

    cursor.execute("INSERT OR IGNORE INTO spiele (spielnummer, spielklasse, spieldatum, heimmannschaft, gastmannschaft, endstand, halbzeitstand) VALUES (?, ?, ?, ?, ?, ?, ?)",
                   (info['spielnummer'], info['spielklasse'], info['spieldatum'], info['heimmannschaft'], info['gastmannschaft'], info['endstand'], info['halbzeitstand']))

    for team_type in ['heim', 'gast']:
        team_name = data['spiel_info'][f'{team_type}mannschaft']
        for spieler in data[f'spieler_{team_type}']:
            cursor.execute("INSERT INTO spieler (spielnummer, mannschaftsname, trikotnummer, name, jahrgang, tore, sieben_meter_tore, sieben_meter_versuche, verwarnung, hinausstellung_1, hinausstellung_2, hinausstellung_3, disqualifikation) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                           (spielnummer, team_name, spieler['trikotnummer'], spieler['name'], spieler['jahrgang'], spieler['tore'], spieler['sieben_meter_versuche'], spieler['sieben_meter_tore'], spieler['verwarnung'], spieler['hinausstellung_1'], spieler['hinausstellung_2'], spieler['hinausstellung_3'], spieler['disqualifikation']))
            for aktion in spieler['aktionen']:
                cursor.execute("INSERT INTO spieler_aktionen (spielnummer, trikotnummer, mannschaftsname, spielzeit, aktionstyp, spielstand) VALUES (?, ?, ?, ?, ?, ?)",
                               (spielnummer, spieler['trikotnummer'], team_name, aktion['spielzeit'], aktion['aktion'], aktion['spielstand']))
    for aktion in data['aktionen_heim']:
        cursor.execute("INSERT INTO mannschafts_aktionen (spielnummer, mannschaftsname, spielzeit, aktionstyp, spielstand) VALUES (?, ?, ?, ?, ?)",
                       (spielnummer, data['spiel_info']['heimmannschaft'], aktion['spielzeit'], aktion['aktion'], aktion['spielstand']))
    for aktion in data['aktionen_gast']:
        cursor.execute("INSERT INTO mannschafts_aktionen (spielnummer, mannschaftsname, spielzeit, aktionstyp, spielstand) VALUES (?, ?, ?, ?, ?)",
                       (spielnummer, data['spiel_info']['gastmannschaft'], aktion['spielzeit'], aktion['aktion'], aktion['spielstand']))
    conn.commit()
    conn.close()
    print(f"Spiel {spielnummer} erfolgreich in die DB eingefügt.")

def get_all_spiele(team_filter=None, spielklasse_filter=None, sort_by='spieldatum', order='DESC'):
    """
    Holt eine Liste aller Spiele aus der Datenbank.
    Kann nach Team und Spielklasse filtern und die Ausgabe sortieren.
    """
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cursor = conn.cursor()

    valid_sort_columns = ['spielnummer', 'spieldatum', 'heimmannschaft', 'gastmannschaft', 'endstand', 'spielklasse']
    if sort_by not in valid_sort_columns:
        sort_by = 'spieldatum'
    if order.upper() not in ['ASC', 'DESC']:
        order = 'DESC'

    query = "SELECT spielnummer, spieldatum, heimmannschaft, gastmannschaft, endstand, spielklasse FROM spiele"
    params = []
    where_clauses = []

    if team_filter:
        where_clauses.append("(heimmannschaft = ? OR gastmannschaft = ?)")
        params.extend([team_filter, team_filter])

    if spielklasse_filter:
        where_clauses.append("spielklasse = ?")
        params.append(spielklasse_filter)

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    query += f" ORDER BY {sort_by} {order}"

    cursor.execute(query, params)
    spiele = cursor.fetchall()
    conn.close()
    return spiele

def get_spiel_details(spielnummer):
    """Stellt die Daten für ein einzelnes Spiel aus der DB wieder her."""
    conn = sqlite3.connect(DB_NAME, timeout=10)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM spiele WHERE spielnummer = ?", (spielnummer,))
    spiel_info_raw = cursor.fetchone()
    if not spiel_info_raw: return None
    spiel_info = dict(spiel_info_raw)

    data = {"spiel_info": spiel_info, "spieler_heim": [], "spieler_gast": [], "aktionen_heim": [], "aktionen_gast": []}

    cursor.execute("SELECT * FROM spieler WHERE spielnummer = ?", (spielnummer,))
    alle_spieler = [dict(p) for p in cursor.fetchall()]

    for spieler in alle_spieler:
        cursor.execute("SELECT * FROM spieler_aktionen WHERE spielnummer = ? AND trikotnummer = ? AND mannschaftsname = ?",
                       (spielnummer, spieler['trikotnummer'], spieler['mannschaftsname']))
        spieler['aktionen'] = [dict(a) for a in cursor.fetchall()]

        if spieler['mannschaftsname'] == spiel_info['heimmannschaft']:
            data['spieler_heim'].append(spieler)
        else:
            data['spieler_gast'].append(spieler)

    cursor.execute("SELECT * FROM mannschafts_aktionen WHERE spielnummer = ?", (spielnummer,))
    for aktion in [dict(a) for a in cursor.fetchall()]:
        if aktion['mannschaftsname'] == spiel_info['heimmannschaft']:
            data['aktionen_heim'].append(aktion)
        else:
            data['aktionen_gast'].append(aktion)

    conn.close()
    return data

def delete_spiel(spielnummer):
    """Löscht ein Spiel und alle zugehörigen Einträge."""
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM mannschafts_aktionen WHERE spielnummer = ?", (spielnummer,))
    cursor.execute("DELETE FROM spieler_aktionen WHERE spielnummer = ?", (spielnummer,))
    cursor.execute("DELETE FROM spieler WHERE spielnummer = ?", (spielnummer,))
    cursor.execute("DELETE FROM spiele WHERE spielnummer = ?", (spielnummer,))
    conn.commit()
    conn.close()
    print(f"Spiel {spielnummer} wurde aus der Datenbank gelöscht.")

def get_unique_player_names_by_team(mannschaftsname):
    """Holt eine Liste aller einzigartigen, echten Spielernamen für ein Team."""
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT name FROM spieler WHERE mannschaftsname = ? AND name NOT LIKE 'Spieler %' AND name NOT LIKE 'N.N.%' ORDER BY name", (mannschaftsname,))
    names = [row[0] for row in cursor.fetchall()]
    conn.close()
    return names

def update_player_name(spielnummer, trikotnummer, mannschaftsname, new_name):
    """Aktualisiert den Namen eines bestimmten Spielers in einem bestimmten Spiel."""
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cursor = conn.cursor()
    cursor.execute("UPDATE spieler SET name = ? WHERE spielnummer = ? AND trikotnummer = ? AND mannschaftsname = ?",
                   (new_name, spielnummer, trikotnummer, mannschaftsname))
    conn.commit()
    conn.close()
    print(f"Spieler #{trikotnummer} in Spiel {spielnummer} zu '{new_name}' umbenannt.")

def get_roster(spielnummer, mannschaftsname):
    """Holt den Kader (Trikotnummer -> Name) für ein bestimmtes Spiel/Team."""
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cursor = conn.cursor()
    cursor.execute("SELECT trikotnummer, name FROM spieler WHERE spielnummer = ? AND mannschaftsname = ? AND name NOT LIKE 'Spieler %' AND name NOT LIKE 'N.N.%'", 
                   (spielnummer, mannschaftsname))
    roster = {row[0]: row[1] for row in cursor.fetchall()}
    conn.close()
    return roster

def apply_roster(target_spielnummer, mannschaftsname, source_roster):
    """Wendet einen Quell-Kader auf ein Ziel-Spiel an."""
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cursor = conn.cursor()
    for trikotnummer, name in source_roster.items():
        cursor.execute("UPDATE spieler SET name = ? WHERE spielnummer = ? AND mannschaftsname = ? AND trikotnummer = ?",
                       (name, target_spielnummer, mannschaftsname, trikotnummer))
    conn.commit()
    conn.close()
    print(f"Kader auf Spiel {target_spielnummer} für Team {mannschaftsname} angewendet.")

def get_spiele_by_team(mannschaftsname):
    """Holt alle Spiele, an denen ein bestimmtes Team beteiligt war."""
    conn = sqlite3.connect(DB_NAME, timeout=10)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM spiele WHERE heimmannschaft = ? OR gastmannschaft = ? ORDER BY spieldatum DESC",
                   (mannschaftsname, mannschaftsname))
    spiele = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return spiele

def get_all_teams():
    """Holt eine alphabetisch sortierte Liste aller einzigartigen Mannschaftsnamen."""
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT heimmannschaft FROM spiele UNION SELECT DISTINCT gastmannschaft FROM spiele ORDER BY heimmannschaft")
    teams = [row[0] for row in cursor.fetchall()]
    conn.close()
    return teams

def get_all_spielklassen():
    """Holt eine Liste aller einzigartigen Spielklassen."""
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT spielklasse FROM spiele WHERE spielklasse IS NOT NULL AND spielklasse != '' ORDER BY spielklasse")
    spielklassen = [row[0] for row in cursor.fetchall()]
    conn.close()
    return spielklassen

def get_team_game_results(mannschaftsname):
    """Holt die Ergebnisse aller Spiele eines Teams für die Sieg/Niederlage-Berechnung."""
    conn = sqlite3.connect(DB_NAME, timeout=10)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT heimmannschaft, gastmannschaft, endstand FROM spiele WHERE heimmannschaft = ? OR gastmannschaft = ?",
                   (mannschaftsname, mannschaftsname))
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results

def get_player_stats_for_team(mannschaftsname):
    """
    Aggregiert Spielerstatistiken für ein Team über alle Spiele.
    Tore und 7m-Werte werden präzise aus der Aktionen-Tabelle berechnet.
    """
    conn = sqlite3.connect(DB_NAME, timeout=10)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    base_query = """
        SELECT
            s.{group_by_columns} as group_key,
            COUNT(DISTINCT s.spielnummer) as spiele,
            SUM(IFNULL(agg_aktionen.total_tore, 0)) as total_tore,
            SUM(IFNULL(agg_aktionen.total_7m_tore, 0)) as total_7m_tore,
            SUM(IFNULL(agg_aktionen.total_7m_versuche, 0)) as total_7m_versuche,
            SUM(CASE WHEN s.verwarnung IS NOT NULL AND s.verwarnung != '' THEN 1 ELSE 0 END) as total_verwarnungen,
            SUM(CASE WHEN s.hinausstellung_1 IS NOT NULL AND s.hinausstellung_1 != '' THEN 1 ELSE 0 END +
                CASE WHEN s.hinausstellung_2 IS NOT NULL AND s.hinausstellung_2 != '' THEN 1 ELSE 0 END +
                CASE WHEN s.hinausstellung_3 IS NOT NULL AND s.hinausstellung_3 != '' THEN 1 ELSE 0 END) as total_hinausstellungen,
            SUM(CASE WHEN s.disqualifikation IS NOT NULL AND s.disqualifikation != '' THEN 1 ELSE 0 END) as total_disqualifikationen
        FROM spieler s
        LEFT JOIN (
            SELECT
                spielnummer, trikotnummer, mannschaftsname,
                SUM(CASE WHEN aktionstyp LIKE '%Tor%' THEN 1 ELSE 0 END) as total_tore,
                SUM(CASE WHEN aktionstyp = '7m-Tor' THEN 1 ELSE 0 END) as total_7m_tore,
                SUM(CASE 
                    WHEN aktionstyp = '7m-Tor' THEN 1
                    WHEN aktionstyp LIKE '%7m%kein Tor%' THEN 1
                    WHEN aktionstyp LIKE '%Fehlwurf%' THEN 1
                    WHEN aktionstyp LIKE '%verworfen%' THEN 1
                    WHEN aktionstyp LIKE '%gehalten%' THEN 1
                    ELSE 0
                END) as total_7m_versuche
            FROM spieler_aktionen
            GROUP BY spielnummer, trikotnummer, mannschaftsname
        ) AS agg_aktionen ON s.spielnummer = agg_aktionen.spielnummer AND s.trikotnummer = agg_aktionen.trikotnummer AND s.mannschaftsname = agg_aktionen.mannschaftsname
        WHERE s.mannschaftsname = ? AND {where_clause}
        GROUP BY group_key
    """

    named_query = base_query.format(group_by_columns="name", where_clause="s.name NOT LIKE 'Spieler %' AND s.name NOT LIKE 'N.N.%'")
    cursor.execute(named_query, (mannschaftsname,))
    named_player_stats = [dict(row) for row in cursor.fetchall()]
    for p in named_player_stats: p['name'] = p['group_key']

    unnamed_query = base_query.format(group_by_columns="trikotnummer", where_clause="(s.name LIKE 'Spieler %' OR s.name LIKE 'N.N.%')")
    cursor.execute(unnamed_query, (mannschaftsname,))
    unnamed_player_stats_raw = [dict(row) for row in cursor.fetchall()]

    unnamed_player_stats = []
    for row in unnamed_player_stats_raw:
        row['name'] = f"N.N. (Trikot Nr. {row['group_key']})"
        unnamed_player_stats.append(row)

    conn.close()

    all_stats = named_player_stats + unnamed_player_stats
    all_stats.sort(key=lambda x: x.get('total_tore', 0) or 0, reverse=True)

    return all_stats