
from __future__ import annotations

import json
# from typing import Any

# import requests

# --- NHL API endpoints ---
SEASON_URL = "https://api.nhle.com/stats/rest/en/season"
TEAMS_URL = "https://api.nhle.com/stats/rest/en/team"
GAMES_URL = "https://api.nhle.com/stats/rest/en/game"
# Play-by-Play
PBP_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"


# -------------------------
# HTTP helper
# -------------------------
import requests
from typing import Any

def _http_get(url: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """Helper function to perform HTTP GET requests."""
    try:
        response = requests.get(url, params=params, timeout=30)  # Set a timeout for the request
        response.raise_for_status()         # Raise HTTPError for bad responses (4xx and 5xx codes)
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred while fetching {url}: {http_err}")  # Log HTTP errors
    except requests.exceptions.RequestException as req_err:
        print(f"Network error while fetching {url}: {req_err}")  # Log other request errors
    except ValueError as json_err:
        print(f"JSON decoding error for {url}: {json_err}")  # Log JSON decoding errors        
    return None     # Return None in case of any error



# ----------------------------
# Low-level mapped fetchers
# (conservant l'API actuelle des autres modules)
# ----------------------------
def _fetch_game_by_id_from_api(idGame: int) -> list[dict[str, Any]]:
    """Fetch game data by game ID from the NHL API."""
    data = _http_get(GAMES_URL, params={"cayenneExp": f"id={idGame}"})
    items = data.get("data", [])
    out: list[dict[str, Any]] = []
    for g in items:
        out.append(
            {
                "game_id": g.get("id") or g.get("gameId"),
                "season_id": g.get("season"),
                "gameDate": g.get("gameDate"),
                "gameType": g.get("gameType"),
                "gameNumber": g.get("gameNumber"),
                "gameScheduleStateId": g.get("gameScheduleStateId"),
                "gameStateId": g.get("gameStateId"),
                "period": g.get("period"),
                "homeTeamId": g.get("homeTeamId"),
                "awayTeamId": g.get("visitingTeamId"),
                "homeScore": g.get("homeScore"),
                "awayScore": g.get("visitingScore"),
                "startTimeUTC": g.get("easternStartTime"),
                "venue": g.get("venue"),
                "venueLocation": g.get("venueLocation"),
            }
        )
    return out

# ----------------------------
# Public helpers for db.py
# (DB n'a plus que des upserts/updates)
# ----------------------------
def get_seasons() -> list[dict[str, Any]]:
    """Retourne la liste brute des saisons (data[])."""
    data = _http_get(SEASON_URL)
    return data.get("data", []) if data else []


def get_teams() -> list[dict[str, Any]]:
    """Retourne la liste brute des équipes (data[])."""
    data = _http_get(TEAMS_URL)
    return data.get("data", []) if data else []


def get_games_for_season(season_id: int) -> list[dict[str, Any]]:
    """Retourne la liste brute des matchs d'une saison (data[])."""
    data = _http_get(GAMES_URL, params={"cayenneExp": f"season={season_id}"})
    return data.get("data", []) if data else []


def _def(d: dict | None) -> Any:
    """Récupère la clé 'default' si présente dans un sous-objet (payload NHL)."""
    if not d:
        return None
    return d.get("default")


def build_roster_rows(season_id: int, team_tricode: str, team_id: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Fetch le roster d'une équipe pour une saison et retourne:
    - players_rows: Lignes prêtes pour INSERT INTo Player
    - roster_rows: Lignes prêtes pour INSERT INTO Roster
    """

    if not team_tricode or not season_id:
        return [], []
    url = f"https://api-web.nhle.com/v1/roster/{team_tricode}/{season_id}"
    data = _http_get(url)

    groups: list[dict] = []
    for k in ("forwards", "defensemen", "goalies"):
        group = data.get(k, [])
        if isinstance(group, list):
            groups.extend(group)
    
    if not groups:
        return [], []
    
    players_rows: list[dict[str, Any]] = []
    roster_rows: list[dict[str, Any]] = []
    
    for player in groups:
        pid = player.get("id")
        if not pid:
            continue
        players_rows.append(
            {
                "player_id": pid,
                "first_name": _def(player.get("firstName")),
                "last_name": _def(player.get("lastName")),
                "headshot:": player.get("headshot"),
                "shootsCatches": player.get("shootsCatches"),
                "positionCode": player.get("positionCode"),
                "birthDate": player.get("birthDate"),
                "birthCity": _def(player.get("birthCity")),
                "birthStateProvince": _def(player.get("birthStateProvince")),
                "birthCountry": player.get("birthCountry"),
                "heightInInches": player.get("heightInInches"),
                "weightInPounds": player.get("weightInPounds"),
                "heightInCentimeters": player.get("heightInCentimeters"),
                "weightInKilograms": player.get("weightInKilograms"),
            }
        )
        roster_rows.append(
            {
                "season_id": season_id,
                "team_id": team_id,
                "player_id": pid,
                "sweater_number": player.get("sweaterNumber"),
                "positionCode": player.get("positionCode"),
            }
        )
    
    return players_rows, roster_rows


def build_pbp_rows(game_id: int) -> dict[str, Any]:
    """
    Fetch le play-by-play d'un match et retourne un dict avec:
      {
        "meta": {startTimeUTC, venue, venueLocation, hasPlays},
        "teams": {homeTeam_raw, awayTeam_raw},
        "events": [Event rows prêts pour INSERT INTO Event],
        "rosterSpots": roster_spots
      }
    """
    data = _http_get(PBP_URL.format(game_id=game_id))

    # Teams (raw) - upsert côté DB
    teams_out: list[dict[str, Any]] = []
    for side in ("homeTeam", "awayTeam"):
        t = data.get(side)
        if t:
            teams_out.append(t)
    
    # Meta pour mise à jour Game
    plays = data.get("plays", [])
    meta = {
        "startTimeUTC": data.get("startTimeUTC"),
        "venue": _def(data.get("venue")),
        "venueLocation": _def(data.get("venueLocation")),
        "hasPlays": 1 if plays else 0
    }

    # Events mapping
    events_rows: list[dict[str, Any]] = []
    roster_spots = data.get("rosterSpots") or []

    for p in plays:
        ev_id = p.get("eventId")
        if ev_id is None:
            continue
        details = p.get("details") or {}
        events_rows.append(
            {
                "game_id": game_id,
                "eventId": ev_id,
                "period": (p.get("periodDesciptor") or {}).get("number"),
                "periodType": (p.get("periodDescriptor") or {}).get("periodType"),
                "timeInPeriod": p.get("timeInPeriod"),
                "timeRemaining": p.get("timeRemaining"),
                "typeCode": p.get("typeCode"),
                "typeDescKey": p.get("typeDesckey"),
                "sortOrder": p.get("sortOrder"),
                "penaltyTypeCode": details.get("typeCode"),
                "penaltyDuration": details.get("duration"),
                "committedByPlayerId": details.get("committedByPlayerId"),
                "eventOwnerTeamId": details.get("eventOwnerTeamId"),
                "details_json": json.dumps(details, ensure_ascii=False),
                "xCoord": details.get("xCoord"),
                "yCoord": details.get("yCoord"),
                "shotType": details.get("shotType"),
                "shootingPlayerId": details.get("scoringPlayerId") or details.get("shootingPlayerId"),
                "goalieInNetId": details.get("goalieInNetId"),
                "zoneCode": details.get("zoneCode"), 
                "situationCode": p.get("situationCode") or (p.get("periodDescriptor") or {}).get("situationCode"),
                "homeTeamDefendingSide": p.get("homeTeamDefendingSide"),
            }
        )

    return {"meta": meta, "teams": teams_out, "events": events_rows, "rosterSpots": roster_spots}



__all__ = [
    # constants + http
    "SEASONS_URL",
    "TEAMS_URL",
    "GAMES_URL",
    "PBP_URL",
    "_http_get",
    # low-level mapped fetchers (legacy API)
    "_fetch_game_by_id_from_api",
    # public helpers for db.py
    "get_seasons",
    "get_teams",
    "get_games_for_season",
    "build_roster_rows",
    "build_pbp_rows",
]