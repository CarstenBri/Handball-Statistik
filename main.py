# main.py
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import pdfplumber
import io
import re
import os
import sqlite3
import database
from datetime import datetime

app = Flask(__name__)
app.config['SECRET_KEY'] = 'dein-super-geheimer-schluessel-12345'

def find_best_team_match(name_from_action, full_heim_name, full_gast_name):
    """
    Findet die beste Übereinstimmung für einen potenziell verkürzten Mannschaftsnamen.
    Gibt den vollständigen Namen des Heim- oder Gastteams zurück.
    """
    if not name_from_action:
        return None

    name_action_lower = name_from_action.lower()
    heim_lower = full_heim_name.lower()
    gast_lower = full_gast_name.lower()

    if name_action_lower == heim_lower or name_action_lower in heim_lower:
        return full_heim_name
    if name_action_lower == gast_lower or name_action_lower in gast_lower:
        return full_gast_name

    words_action = set(name_action_lower.split())
    words_heim = set(heim_lower.split())
    words_gast = set(gast_lower.split())

    score_heim = len(words_action.intersection(words_heim))
    score_gast = len(words_action.intersection(words_gast))

    if score_heim > score_gast:
        return full_heim_name
    if score_gast > score_heim:
        return full_gast_name

    return None

def parse_aktion(aktion_string):
    """
    Zerlegt eine Aktions-Zeichenkette in ihre Bestandteile.
    Kann Formate wie "(17, Team)" und "Spieler 7, Team" verarbeiten.
    """
    parsed_data = {"aktionstyp": None, "spieler_name": None, "trikotnummer": None, "mannschaftsname": None}
    if not aktion_string:
        return parsed_data

    aktion_string = aktion_string.strip()

    paren_match = re.search(r'\((\d{1,2}),\s*(.*?)\)$', aktion_string)
    if paren_match:
        parsed_data["trikotnummer"] = paren_match.group(1)
        parsed_data["mannschaftsname"] = paren_match.group(2).strip()
        main_action_part = aktion_string[:paren_match.start()].strip()

        for sep in [" durch ", " für ", " von "]:
            if sep in main_action_part:
                parts = main_action_part.split(sep, 1)
                parsed_data["aktionstyp"] = parts[0].strip()
                parsed_data["spieler_name"] = parts[1].strip()
                return parsed_data

        parsed_data["aktionstyp"] = main_action_part
        return parsed_data

    for sep in [" durch ", " für ", " von "]:
        if sep in aktion_string:
            parts = aktion_string.split(sep, 1)
            parsed_data["aktionstyp"] = parts[0].strip()
            rest_string = parts[1].strip()

            spieler_match = re.search(r'Spieler\s*(\d{1,2}),\s*(.*)', rest_string)
            if spieler_match:
                parsed_data["trikotnummer"] = spieler_match.group(1)
                parsed_data["mannschaftsname"] = spieler_match.group(2).strip()
                parsed_data["spieler_name"] = f"Spieler {parsed_data['trikotnummer']}"

            return parsed_data

    parts = aktion_string.split(' ', 1)
    parsed_data["aktionstyp"] = parts[0]
    if len(parts) > 1:
        parsed_data["mannschaftsname"] = parts[1].strip()
    return parsed_data

def parse_player_row(row):
    """Verarbeitet eine einzelne Spielerzeile aus einer Tabelle."""
    if not row or row[0] is None or not row[0].isdigit() or len(row) < 12:
        return None

    sieben_meter_komplett = row[6] or ""
    sieben_meter_tore, sieben_meter_versuche = ("", "")
    if '/' in sieben_meter_komplett:
        teile = sieben_meter_komplett.split('/')
        sieben_meter_versuche = teile[0].strip()
        sieben_meter_tore = teile[1].strip()

    return {
        "trikotnummer": row[0], "name": row[1], "jahrgang": row[2], "tore": row[5],
        "sieben_meter_tore": sieben_meter_tore, "sieben_meter_versuche": sieben_meter_versuche,
        "verwarnung": row[7], "hinausstellung_1": row[8], "hinausstellung_2": row[9],
        "hinausstellung_3": row[10], "disqualifikation": row[11], "aktionen": []
    }

def parse_pdf_data(file_stream):
    """Extrahiert alle relevanten Daten aus dem PDF-Stream zu einem Dictionary."""
    data = {
        "spiel_info": {"spielklasse": "n.g.", "spielnummer": "n.g.", "spieldatum": "n.g.", "heimmannschaft": "n.g.", "gastmannschaft": "n.g.", "endstand": "n.g.", "halbzeitstand": "n.g."},
        "spieler_heim": [], "spieler_gast": [], "aktionen_heim": [], "aktionen_gast": [],
    }
    with pdfplumber.open(file_stream) as pdf:
        if pdf.pages:
            try:
                for table in pdf.pages[0].extract_tables():
                    for row in table:
                        if row and row[0] and "Spiel/Datum" in row[0] and row[1]:
                            parts = row[1].split(',')
                            data["spiel_info"]["spielnummer"] = parts[0].strip()
                            # Datumsformat für korrekte Sortierung anpassen
                            raw_date = parts[1].split(' am ')[1].split(' um')[0].strip()
                            dt_object = None
                            try:
                                dt_object = datetime.strptime(raw_date, '%d.%m.%Y')
                            except ValueError:
                                try:
                                    dt_object = datetime.strptime(raw_date, '%d.%m.%y')
                                except ValueError:
                                    pass # dt_object bleibt None

                            if dt_object:
                                data["spiel_info"]["spieldatum"] = dt_object.strftime('%Y-%m-%d')
                            else:
                                data["spiel_info"]["spieldatum"] = "n.g."
                            break
                    if data["spiel_info"]["spielnummer"] != "n.g.":
                        break
            except (ValueError, IndexError):
                pass

        full_text = "".join([p.extract_text(x_tolerance=2, y_tolerance=2) or "" for p in pdf.pages])

        for page in pdf.pages:
            if page.page_number > 2:
                continue
            gast_y_pos = None
            try:
                for word in page.extract_words(use_text_flow=True):
                    if 'gast' in word['text'].lower():
                        gast_y_pos = word['top']
                        break
            except Exception:
                pass

            extracted_tables, found_tables = page.extract_tables(), page.find_tables()
            if len(extracted_tables) != len(found_tables):
                continue

            for i, table_data in enumerate(extracted_tables):
                is_guest_table = gast_y_pos is not None and found_tables[i].bbox[1] > gast_y_pos
                target_list = data["spieler_gast"] if is_guest_table else data["spieler_heim"]
                for row in table_data:
                    player_data = parse_player_row(row)
                    if player_data and not any(p['trikotnummer'] == player_data['trikotnummer'] for p in target_list):
                        target_list.append(player_data)

        for line in full_text.splitlines():
            line_strip = line.strip()
            if line_strip.startswith('Spielklasse'):
                data["spiel_info"]["spielklasse"] = line.split(':', 1)[1].strip()
            elif line_strip.startswith('Heim:'):
                data["spiel_info"]["heimmannschaft"] = line.split(':', 1)[1].strip()
            elif line_strip.startswith('Gast:'):
                data["spiel_info"]["gastmannschaft"] = line.split(':', 1)[1].strip()
            elif 'Endstand' in line:
                match = re.search(r'Endstand\s*([\d\s:]+)\s*\((\d+:\d+)\)', line)
                if match:
                    data["spiel_info"]["endstand"], data["spiel_info"]["halbzeitstand"] = match.group(1).strip(), match.group(2).strip()

        player_map, heim_name, gast_name = {}, data["spiel_info"]["heimmannschaft"], data["spiel_info"]["gastmannschaft"]
        for p in data["spieler_heim"]:
            player_map[(heim_name, p["trikotnummer"])] = p
        for p in data["spieler_gast"]:
            player_map[(gast_name, p["trikotnummer"])] = p

        if len(pdf.pages) > 2:
            for page in pdf.pages[2:]:
                for table in page.extract_tables():
                    for row in table:
                        if not row or len(row) < 4 or not row[3]:
                            continue
                        spielzeit, spielstand, aktion_string = row[1], row[2], row[3]
                        parsed_details = parse_aktion(aktion_string)

                        team_context = find_best_team_match(parsed_details["mannschaftsname"], heim_name, gast_name)
                        if not team_context:
                            continue

                        event = {"spielzeit": spielzeit, "aktion": parsed_details["aktionstyp"], "spielstand": spielstand}
                        if parsed_details["trikotnummer"]:
                            player_to_update = player_map.get((team_context, parsed_details["trikotnummer"]))
                            if player_to_update and parsed_details["aktionstyp"]:
                                player_to_update["aktionen"].append(event)
                        elif parsed_details["aktionstyp"]:
                            if team_context == heim_name:
                                data["aktionen_heim"].append(event)
                            else:
                                data["aktionen_gast"].append(event)
    return data

@app.route('/')
def index():
    """Zeigt eine Liste aller gespeicherten Spiele an, mit Filter- und Sortieroptionen."""
    team_filter = request.args.get('team_filter', None)
    spielklasse_filter = request.args.get('spielklasse_filter', None)
    sort_by = request.args.get('sort_by', 'spieldatum')
    default_order = 'asc' if sort_by in ['heimmannschaft', 'gastmannschaft', 'spielklasse'] else 'desc'
    order = request.args.get('order', default_order)

    if team_filter == "alle":
        team_filter = None
    if spielklasse_filter == "alle":
        spielklasse_filter = None

    spiele_raw = database.get_all_spiele(team_filter, spielklasse_filter, sort_by, order)

    spiele_for_template = []
    for spiel in spiele_raw:
        # spiel: (spielnummer, spieldatum_db, heim, gast, endstand, spielklasse_voll)
        spieldatum_db = spiel[1]
        display_date = spieldatum_db
        try:
            # Konvertiere YYYY-MM-DD zurück zu DD.MM.YYYY für die Anzeige
            display_date = datetime.strptime(spieldatum_db, '%Y-%m-%d').strftime('%d.%m.%Y')
        except (ValueError, TypeError):
            pass # Behalte das Datum aus der DB bei, wenn es nicht dem Format entspricht

        full_spielklasse = spiel[5] or ""
        abbreviation = ""
        match = re.search(r'\((.*?)\)', full_spielklasse)
        if match:
            abbreviation = match.group(1)

        spiele_for_template.append((spiel[0], display_date, spiel[2], spiel[3], spiel[4], abbreviation))

    spiel_count = len(spiele_for_template)
    teams = database.get_all_teams()
    spielklassen = database.get_all_spielklassen()

    next_order = 'desc' if order == 'asc' else 'asc'

    return render_template('index.html', 
                           spiele=spiele_for_template,
                           spiel_count=spiel_count,
                           teams=teams,
                           spielklassen=spielklassen,
                           current_team_filter=team_filter,
                           current_spielklasse_filter=spielklasse_filter,
                           sort_by=sort_by,
                           next_order=next_order)


@app.route('/upload', methods=['POST'])
def upload_file():
    """Nimmt eine oder mehrere PDFs entgegen, parst sie und speichert sie in der DB."""
    if 'pdf_file' not in request.files:
        flash('Kein Dateiteil im Request gefunden.', 'error')
        return redirect(url_for('index'))

    files = request.files.getlist('pdf_file')

    if not files or files[0].filename == '':
        flash('Keine Dateien ausgewählt.', 'warning')
        return redirect(url_for('index'))

    success_count = 0
    error_count = 0
    warning_count = 0

    for file in files:
        if file and file.filename.endswith('.pdf'):
            try:
                file.seek(0)
                file_stream = io.BytesIO(file.read())
                extracted_data = parse_pdf_data(file_stream)
                database.insert_spielbericht_data(extracted_data)
                success_count += 1
            except sqlite3.IntegrityError:
                warning_count += 1
            except Exception as e:
                error_count += 1
                print(f"Fehler bei Datei {file.filename}: {e}")
        else:
            error_count += 1

    if success_count > 0:
        flash(f'{success_count} Spielbericht(e) erfolgreich importiert.', 'success')
    if warning_count > 0:
        flash(f'{warning_count} Spielbericht(e) war(en) bereits in der Datenbank vorhanden.', 'warning')
    if error_count > 0:
        flash(f'{error_count} Datei(en) konnten nicht verarbeitet werden.', 'error')

    return redirect(url_for('index'))

@app.route('/spiel/<spielnummer>')
def spiel_detail(spielnummer):
    """Zeigt die Detailansicht für ein einzelnes, aus der DB geladenes Spiel."""
    data = database.get_spiel_details(spielnummer)
    if data is None:
        flash('Spiel nicht gefunden.', 'error')
        return redirect(url_for('index'))

    heim_namen = database.get_unique_player_names_by_team(data['spiel_info']['heimmannschaft'])
    gast_namen = database.get_unique_player_names_by_team(data['spiel_info']['gastmannschaft'])

    heim_spiele = database.get_spiele_by_team(data['spiel_info']['heimmannschaft'])
    gast_spiele = database.get_spiele_by_team(data['spiel_info']['gastmannschaft'])

    heim_spiele_vorlagen = [s for s in heim_spiele if s['spielnummer'] != spielnummer]
    gast_spiele_vorlagen = [s for s in gast_spiele if s['spielnummer'] != spielnummer]

    # Datum für die Anzeige formatieren
    try:
        data['spiel_info']['spieldatum'] = datetime.strptime(data['spiel_info']['spieldatum'], '%Y-%m-%d').strftime('%d.%m.%Y')
    except (ValueError, TypeError):
        pass

    return render_template('result.html', data=data, heim_namen=heim_namen, gast_namen=gast_namen,
                           heim_spiele_vorlagen=heim_spiele_vorlagen, gast_spiele_vorlagen=gast_spiele_vorlagen)

@app.route('/spiel/loeschen/<spielnummer>', methods=['POST'])
def delete_spiel_route(spielnummer):
    """Löscht ein Spiel aus der Datenbank."""
    database.delete_spiel(spielnummer)
    flash(f'Spiel {spielnummer} wurde erfolgreich gelöscht.', 'success')
    return redirect(url_for('index'))

@app.route('/spieler/update', methods=['POST'])
def update_spieler():
    """Aktualisiert einen Spielernamen via AJAX."""
    try:
        req_data = request.get_json()
        database.update_player_name(
            spielnummer=req_data['spielnummer'],
            trikotnummer=req_data['trikotnummer'],
            mannschaftsname=req_data['mannschaftsname'],
            new_name=req_data['newName']
        )
        return jsonify(success=True)
    except Exception as e:
        print(f"Update fehlgeschlagen: {e}")
        return jsonify(success=False, error=str(e)), 500

@app.route('/spiel/kopiere_kader', methods=['POST'])
def kopiere_kader_route():
    """Kopiert den Kader von einem Spiel zum anderen."""
    try:
        target_spielnummer = request.form['target_spielnummer']
        mannschaftsname = request.form['mannschaftsname']
        source_spielnummer = request.form['source_spielnummer']

        source_roster = database.get_roster(source_spielnummer, mannschaftsname)

        if source_roster:
            database.apply_roster(target_spielnummer, mannschaftsname, source_roster)
            flash('Kader wurde erfolgreich kopiert!', 'success')
        else:
            flash('Der Quell-Kader war leer oder konnte nicht geladen werden.', 'warning')

    except Exception as e:
        flash(f'Fehler beim Kopieren des Kaders: {e}', 'error')

    return redirect(url_for('spiel_detail', spielnummer=target_spielnummer))

@app.route('/statistik')
def statistik_index():
    """Zeigt eine Liste aller Teams für die Statistik-Auswahl an."""
    teams = database.get_all_teams()
    return render_template('statistik_auswahl.html', teams=teams)

@app.route('/statistik/<path:mannschaftsname>')
def team_statistik(mannschaftsname):
    """Zeigt die aggregierten Statistiken für ein ausgewähltes Team an."""
    player_stats = database.get_player_stats_for_team(mannschaftsname)
    game_results = database.get_team_game_results(mannschaftsname)

    team_stats = {
        "spiele": len(game_results), "siege": 0, "unentschieden": 0, "niederlagen": 0,
        "tore_geschossen": 0, "tore_kassiert": 0
    }

    for game in game_results:
        try:
            heim_tore, gast_tore = map(int, game['endstand'].replace(' ', '').split(':'))
            if game['heimmannschaft'] == mannschaftsname:
                team_stats['tore_geschossen'] += heim_tore
                team_stats['tore_kassiert'] += gast_tore
                if heim_tore > gast_tore: team_stats['siege'] += 1
                elif heim_tore < gast_tore: team_stats['niederlagen'] += 1
                else: team_stats['unentschieden'] += 1
            else:
                team_stats['tore_geschossen'] += gast_tore
                team_stats['tore_kassiert'] += heim_tore
                if gast_tore > heim_tore: team_stats['siege'] += 1
                elif gast_tore < heim_tore: team_stats['niederlagen'] += 1
                else: team_stats['unentschieden'] += 1
        except (ValueError, IndexError):
            continue

    return render_template('statistik_team.html', mannschaftsname=mannschaftsname,
                           team_stats=team_stats, player_stats=player_stats)


if __name__ == '__main__':
    if not os.path.exists(database.DB_NAME):
        database.init_db()
    app.run(host='0.0.0.0', port=5000, debug=True)