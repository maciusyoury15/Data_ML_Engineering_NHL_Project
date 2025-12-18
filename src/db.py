from email.mime import base
import json
import sqlite3
from pathlib import Path
from typing import Any

from tqdm import tqdm

from api import (
    PBP_URL as API_PBP,
)
from api import (
    _http_get as get_json,
)
from api import (
    build_pbp_rows,
    build_roster_rows,
    get_games_for_season,
    get_seasons,
    get_teams,
)

DB_PATH = Path(__file__).parent / "nhl.db"


# Optional: focus on a specific season to ensure play-by-play exists and to reduce volume
MAX_GAMES_PBP: int = 10000  # how many games to fetch PBP for

# Seasons to ingest: from 2016-2017 through 2023-2024 (inclusive)
SEASONS = [int(f"{y}{y + 1}") for y in range(2016, 2024)]


def table_has_rows(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute(f"SELECT 1 FROM {table} LIMIT 1;")
    return cur.fetchone() is not None


def existing_ids(conn: sqlite3.Connection, table: str, id_col: str) -> set:
    cur = conn.execute(f"SELECT {id_col} FROM {table};")
    return {row[0] for row in cur.fetchall()}


def game_has_events(conn: sqlite3.Connection, game_id: int) -> bool:
    cur = conn.execute("SELECT 1 FROM Event WHERE game_id=? LIMIT 1;", (game_id,))
    return cur.fetchone() is not None


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS Season (
      season_id INTEGER PRIMARY KEY,
      formattedSeasonId TEXT,
      startDate TEXT,
      endDate TEXT,
      regularSeasonEndDate TEXT,
      preseasonStartdate TEXT,
      numberOfGames INTEGER,
      totalRegularSeasonGames INTEGER,
      totalPlayoffGames INTEGER,
      seasonOrdinal INTEGER,
      conferencesInUse INTEGER,
      divisionsInUse INTEGER,
      wildcardInUse INTEGER,
      tiesInUse INTEGER,
      pointForOTLossInUse INTEGER,
      rowInUse INTEGER,
      allStarGameInUse INTEGER,
      entryDraftInUse INTEGER,
      supplementalDraftInUse INTEGER,
      nhlStanleyCupOwner INTEGER,
      minimumPlayoffMinutesForGoalieStatsLeaders INTEGER,
      minimumRegularGamesForGoalieStatsLeaders INTEGER,
      olympicsParticipation INTEGER
    );

    CREATE TABLE IF NOT EXISTS Team (
      team_id     INTEGER PRIMARY KEY,
      franchiseId INTEGER,
      fullName    TEXT,
      leagueId    INTEGER,
      rawTricode  TEXT,
      triCode     TEXT
    );

    CREATE TABLE IF NOT EXISTS Game (
      game_id             INTEGER PRIMARY KEY,
      season_id           INTEGER,
      gameDate            TEXT,
      gameType            INTEGER,
      gameNumber          INTEGER,
      gameScheduleStateId INTEGER,
      gameStateId         INTEGER,
      period              INTEGER,
      homeTeamId          INTEGER,
      awayTeamId          INTEGER,
      homeScore           INTEGER,
      awayScore           INTEGER,
      startTimeUTC        TEXT,
      venue               TEXT,
      venueLocation       TEXT,
      hasPlays          INTEGER,
      FOREIGN KEY (season_id)  REFERENCES Season(season_id),
      FOREIGN KEY (homeTeamId) REFERENCES Team(team_id),
      FOREIGN KEY (awayTeamId) REFERENCES Team(team_id)
    );

    CREATE INDEX IF NOT EXISTS idx_game_season ON Game(season_id);
    CREATE INDEX IF NOT EXISTS idx_game_home   ON Game(homeTeamId);
    CREATE INDEX IF NOT EXISTS idx_game_away   ON Game(awayTeamId);

    CREATE TABLE IF NOT EXISTS Event (
      game_id          INTEGER NOT NULL,
      eventId          INTEGER NOT NULL,
      period           INTEGER,
      periodType       TEXT,
      timeInPeriod     TEXT,
      timeRemaining    TEXT,
      typeCode         INTEGER,
      typeDescKey      TEXT,
      sortOrder        INTEGER,
      penaltyTypeCode  TEXT,
      penaltyDuration  INTEGER,
      committedByPlayerId INTEGER,
      eventOwnerTeamId INTEGER,
      details_json     TEXT,
      xCoord           REAL,
      yCoord           REAL,
      shotType         TEXT,
      shootingPlayerId INTEGER,
      goalieInNetId    INTEGER,
      zoneCode         TEXT,
      emptyNet         INTEGER,
      PRIMARY KEY (game_id, eventId),
      FOREIGN KEY (game_id) REFERENCES Game(game_id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_event_game  ON Event(game_id);
    CREATE INDEX IF NOT EXISTS idx_event_type  ON Event(typeCode);
    CREATE INDEX IF NOT EXISTS idx_event_team  ON Event(eventOwnerTeamId);

    CREATE TABLE IF NOT EXISTS Player (
      player_id          INTEGER PRIMARY KEY,
      firstName          TEXT,
      lastName           TEXT,
      headshot           TEXT,
      shootsCatches      TEXT,
      positionCode       TEXT,
      birthDate          TEXT,
      birthCity          TEXT,
      birthStateProvince TEXT,
      birthCountry       TEXT,
      heightInInches     INTEGER,
      weightInPounds     INTEGER,
      heightInCentimeters INTEGER,
      weightInKilograms   INTEGER
    );

    CREATE TABLE IF NOT EXISTS Roster (
      season_id  INTEGER NOT NULL,
      team_id    INTEGER NOT NULL,
      player_id  INTEGER NOT NULL,
      sweaterNumber INTEGER,
      positionCode  TEXT,
      PRIMARY KEY (season_id, team_id, player_id),
      FOREIGN KEY (season_id) REFERENCES Season(season_id) ON DELETE CASCADE,
      FOREIGN KEY (team_id)   REFERENCES Team(team_id)   ON DELETE CASCADE,
      FOREIGN KEY (player_id) REFERENCES Player(player_id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_roster_team   ON Roster(team_id);
    CREATE INDEX IF NOT EXISTS idx_roster_player ON Roster(player_id);
    """)
    conn.commit()

    # Lightweight migration: add hasPlays if missing
    cur = conn.execute("PRAGMA table_info(Game);")
    cols = {row[1] for row in cur.fetchall()}
    if "hasPlays" not in cols:
        conn.execute("ALTER TABLE Game ADD COLUMN hasPlays INTEGER;")
        conn.commit()

    # Migration: add shot-specific columns to Event if missing
    cur = conn.execute("PRAGMA table_info(Event);")
    event_cols = {row[1] for row in cur.fetchall()}
    new_shot_cols = [
        "xCoord",
        "yCoord",
        "shotType",
        "shootingPayerId",
        "goalieInNetId",
        "emptyNet"
        "zoneCode",
    ]

    # After you built event_cols
    if "homeTeamDefendingSide" not in event_cols:
        conn.execute("ALTER TABLE Event ADD COLUMN homeTeamDefendingSide TEXT;")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_event_home_team_def_side ON Event(homeTeamDefendingSide);"
        )
        conn.commit()
    if "situationCode" not in event_cols:
        conn.execute("ALTER TABLE Event ADD COLUMN situationCode TEXT;")
        conn.commit()
    
    for col in new_shot_cols:
        if col not in event_cols:
            col_type = (
                "REAL"
                if col in ["xCoord", "yCoord"]
                else (
                    "INTEGER"
                    if col in ["shootingPlayerId", "goalieInNetId", "emptyNet"]
                    else "TEXT"
                )
            )
            conn.execute(f"ALTER TABLE Event ADD COLUMN {col} {col_type};")
    conn.commit()

    # Create indexes on new columns if they don't exist
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_event_shooter ON Event(shootingPlayerId);"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_event_goalie ON Event(goalieInNetId);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_event_typedesc ON Event(typeDescKey);")
    conn.commit()


def backfill_shot_columns_if_needed(conn:sqlite3.Connection, pbar=None) -> None:
    """Backfill shot-specific columns from details_json if they're empty."""
    # Quick check if any events need migration
    cur = conn.execute(
        """
        SELECT 1
        FROM Event
        WHERE typeDescKey IN ('shot-on-goal', 'goal', 'missed-shot', 'blocked-shot')
        AND details_json IS NOT NULL
        AND xCoord IS NULL
        LIMIT 1
        """
    )
    if not cur.fetchone():
        return   # Nothing to migrate
    
    # Count total events that need migration
    cur = conn.execute(
        """
        SELECT COUNT(*)
        FROM Event
        WHERE typeDescKey IN ('shot-on-goal', 'goal', 'missed-shot', 'blocked-shot')
        AND details_json IS NOT NULL
        AND xCoord IS NULL
        """
    )
    total = cur.fetchone()[0]

    print(f"\n Migrating {total:,} shots events from JSON to columns...")

    # Process in batches with progress bar
    batch_size = 10000
    migration_pbar = tqdm(
        total=total,
        desc="Shot migration",
        unit=" events",
        unit_scale=False,
        dynamic_ncols=True,
    )

    processed = 0
    skipped = 0

    while True:
        # Fetch a batch of events that need migration
        cur = conn.execute(
            """
            SELECT game_id, eventId, details_json
            FROM Event
            WHERE typeDescKey IN ('shot-on-goal', 'goal', 'missed-shot', 'blocked-shot')
            AND details_json IS NOT NULL
            AND xCoord IS NULL
            LIMIT ?
            """,
            (batch_size,),
        )

        rows = cur.fetchall()
        if not rows:
            break

        # Parse JSON and prepare updates
        updates = []

        for game_id, event_id, details_json in rows:
            try:
                details = json.loads(details_json) if details_json else {}
            except json.JSONDecodeError:
                details = {}
            
            # Extract coordinates, using 0 as sentinel for missing values
            # This ensures xCoord is NEVER set to NULL
            x_coord = details.get("xCoord")
            y_coord = details.get("yCoord")

            # Track events without ANY coordinates for reporting
            if x_coord is None and y_coord is None:
                skipped += 1
            
            updates.append(
                (
                    x_coord if x_coord is not None else 0,  # 0 = missing coordinate
                    y_coord if y_coord is not None else 0,  # 0 = missing coordinate
                    details.get("shotType"),
                    details.get("scoringPlayerId") or details.get("shootingPlayerId"),
                    details.get("goalieInNetId"),
                    details.get("zoneCode"),
                    1 if details.get("emptyNet") else None,
                    game_id,
                    event_id,
                )
            )

        # Batch update all events
        with conn:
            conn.executemany(
                """
                UPDATE Event
                SET xCoord = ?,
                    yCoord = ?,
                    shotType = ?,
                    shootingPlayerId = ?,
                    goalieInNetId = ?,
                    zoneCode = ?,
                    emptyNet = ?
                WHERE game_id = ? AND eventId = ?
                """,
                updates,
            )

        processed += len(rows)
        migration_pbar.update(len(rows))

    migration_pbar.close()


def _def(x):
    # Helper pour lire .get("default") en sécurité
    return (x or {}).get("default") if isinstance(x, dict) else None


def upsert_players_from_pbp_roster(
    conn: sqlite3.Connection, season_id: int, roster_spots: list[dict]
) -> None:
    if not roster_spots:
        return

    players_rows, roster_rows = [], []

    for r in roster_spots:
        pid = r.get("playerId")
        tid = r.get("teamId")
        if not pid or not tid:
            continue

        players_rows.append(
            {
                "player_id": pid,
                "firstName": _def(r.get("firstName")),
                "lastName": _def(r.get("lastName")),
                "headshot": r.get("headshot"),
                "shootsCatches": r.get("shootsCatches"),
                "positionCode": r.get("positionCode"),
                "birthDate": r.get("birthDate"),
                "birthCity": _def(r.get("birthCity")),
                "birthStateProvince": _def(r.get("birthStateProvince")),
                "birthCountry": r.get("birthCountry"),
                "heightInInches": r.get("heightInInches"),
                "weightInPounds": r.get("weightInPounds"),
                "heightInCentimeters": r.get("heightInCentimeters"),
                "weightInKilograms": r.get("weightInKilograms"),
            }
        )

        roster_rows.append(
            {
                "season_id": season_id,
                "team_id": tid,
                "player_id": pid,
                "sweaterNumber": r.get("sweaterNumber"),
                "positionCode": r.get("positionCode"),
            }
        )

    if not players_rows and not roster_rows:
        return

    with conn:
        conn.executemany(
            """
            INSERT INTO Player(
              player_id, firstName, lastName, headshot, shootsCatches, positionCode,
              birthDate, birthCity, birthStateProvince, birthCountry,
              heightInInches, weightInPounds, heightInCentimeters, weightInKilograms
            ) VALUES (
              :player_id, :firstName, :lastName, :headshot, :shootsCatches, :positionCode,
              :birthDate, :birthCity, :birthStateProvince, :birthCountry,
              :heightInInches, :weightInPounds, :heightInCentimeters, :weightInKilograms
            )
            ON CONFLICT(player_id) DO UPDATE SET
              firstName=COALESCE(excluded.firstName, firstName),
              lastName =COALESCE(excluded.lastName, lastName),
              headshot =COALESCE(excluded.headshot, headshot),
              shootsCatches=COALESCE(excluded.shootsCatches, shootsCatches),
              positionCode =COALESCE(excluded.positionCode, positionCode)
        """,
            players_rows,
        )

        conn.executemany(
            """
            INSERT INTO Roster(season_id, team_id, player_id, sweaterNumber, positionCode)
            VALUES (:season_id, :team_id, :player_id, :sweaterNumber, :positionCode)
            ON CONFLICT(season_id, team_id, player_id) DO UPDATE SET
              sweaterNumber=COALESCE(excluded.sweaterNumber, sweaterNumber),
              positionCode =COALESCE(excluded.positionCode, positionCode)
        """,
            roster_rows,
        )


def upsert_seasons(conn: sqlite3.Connection, seasons: list[dict[str, Any]]) -> None:
    sql = """
    INSERT INTO Season(
      season_id, formattedSeasonId, startDate, endDate, regularSeasonEndDate, preseasonStartdate,
      numberOfGames, totalRegularSeasonGames, totalPlayoffGames, seasonOrdinal,
      conferencesInUse, divisionsInUse, wildcardInUse, tiesInUse, pointForOTLossInUse, rowInUse,
      allStarGameInUse, entryDraftInUse, supplementalDraftInUse, nhlStanleyCupOwner,
      minimumPlayoffMinutesForGoalieStatsLeaders, minimumRegularGamesForGoalieStatsLeaders,
      olympicsParticipation
    )
    VALUES (
      :id, :formattedSeasonId, :startDate, :endDate, :regularSeasonEndDate, :preseasonStartdate,
      :numberOfGames, :totalRegularSeasonGames, :totalPlayoffGames, :seasonOrdinal,
      :conferencesInUse, :divisionsInUse, :wildcardInUse, :tiesInUse, :pointForOTLossInUse, :rowInUse,
      :allStarGameInUse, :entryDraftInUse, :supplementalDraftInUse, :nhlStanleyCupOwner,
      :minimumPlayoffMinutesForGoalieStatsLeaders, :minimumRegularGamesForGoalieStatsLeaders,
      :olympicsParticipation
    )
    ON CONFLICT(season_id) DO UPDATE SET
      formattedSeasonId=excluded.formattedSeasonId,
      startDate=excluded.startDate,
      endDate=excluded.endDate,
      regularSeasonEndDate=excluded.regularSeasonEndDate,
      preseasonStartdate=excluded.preseasonStartdate,
      numberOfGames=excluded.numberOfGames,
      totalRegularSeasonGames=excluded.totalRegularSeasonGames,
      totalPlayoffGames=excluded.totalPlayoffGames,
      seasonOrdinal=excluded.seasonOrdinal,
      conferencesInUse=excluded.conferencesInUse,
      divisionsInUse=excluded.divisionsInUse,
      wildcardInUse=excluded.wildcardInUse,
      tiesInUse=excluded.tiesInUse,
      pointForOTLossInUse=excluded.pointForOTLossInUse,
      rowInUse=excluded.rowInUse,
      allStarGameInUse=excluded.allStarGameInUse,
      entryDraftInUse=excluded.entryDraftInUse,
      supplementalDraftInUse=excluded.supplementalDraftInUse,
      nhlStanleyCupOwner=excluded.nhlStanleyCupOwner,
      minimumPlayoffMinutesForGoalieStatsLeaders=excluded.minimumPlayoffMinutesForGoalieStatsLeaders,
      minimumRegularGamesForGoalieStatsLeaders=excluded.minimumRegularGamesForGoalieStatsLeaders,
      olympicsParticipation=excluded.olympicsParticipation;
    """
    with conn:
        conn.executemany(sql, seasons)


def upsert_teams(conn: sqlite3.Connection, teams: list[dict[str, Any]]) -> None:
    sql = """
    INSERT INTO Team(team_id, franchiseId, fullName, leagueId, rawTricode, triCode)
    VALUES (:id, :franchiseId, :fullName, :leagueId, :rawTricode, :triCode)
    ON CONFLICT(team_id) DO UPDATE SET
      franchiseId=excluded.franchiseId,
      fullName=excluded.fullName,
      leagueId=excluded.leagueId,
      rawTricode=excluded.rawTricode,
      triCode=excluded.triCode;
    """
    with conn:
        conn.executemany(sql, teams)


def ensure_season(conn: sqlite3.Connection, season_id: int | None) -> None:
    if season_id is None:
        return
    with conn:
        conn.execute("INSERT OR IGNORE INTO Season(season_id) VALUES (?)", (season_id,))


def ensure_team(conn: sqlite3.Connection, team_id: int) -> None:
    if team_id is None:
        return
    with conn:
        conn.execute("INSERT OR IGNORE INTO Team(team_id) VALUES (?)", (team_id,))


def ensure_team_roster(conn, season_id: int, team_id: int) -> None:
    if not season_id or not team_id:
        return
    cur = conn.cursor()
    # a) si on a déjà un roster pour (saison, équipe) -> rien à faire
    has = cur.execute(
        "SELECT 1 FROM Roster WHERE season_id=? AND team_id=? LIMIT 1;",
        (season_id, team_id),
    ).fetchone()
    if has:
        return
    # b) récupérer le tricode (Team peut avoir Tricdoes ou RawTricode)
    row = cur.execute(
        "SELECT COALESCE(triCode, rawTricode) AS tc FROM Team WHERE team_id=?",
        (team_id,),
    ).fetchone()
    row = {k: v for k, v in zip(("tc",), row)} if row else None
    if not row or not row["tc"]:
        return
    tricode = row["tc"]

    players_rows, roster_rows = build_roster_rows(season_id, tricode, team_id)
    if not players_rows and not roster_rows:
        return

    # upsert players
    cur.executemany(
        """
        INSERT INTO Player(
          player_id, firstName, lastName, headshot, shootsCatches, positionCode,
          birthDate, birthCity, birthStateProvince, birthCountry,
          heightInInches, weightInPounds, heightInCentimeters, weightInKilograms
        ) VALUES (
          :player_id, :firstName, :lastName, :headshot, :shootsCatches, :positionCode,
          :birthDate, :birthCity, :birthStateProvince, :birthCountry,
          :heightInInches, :weightInPounds, :heightInCentimeters, :weightInKilograms
        )
        ON CONFLICT(player_id) DO UPDATE SET
          firstName=COALESCE(excluded.firstName, firstName),
          lastName =COALESCE(excluded.lastName, lastName),
          headshot =COALESCE(excluded.headshot, headshot),
          shootsCatches=COALESCE(excluded.shootsCatches, shootsCatches),
          positionCode =COALESCE(excluded.positionCode, positionCode)
    """,
        players_rows,
    )

    # upsert roster
    cur.executemany(
        """
        INSERT INTO Roster(season_id, team_id, player_id, sweaterNumber, positionCode)
        VALUES (:season_id, :team_id, :player_id, :sweaterNumber, :positionCode)
        ON CONFLICT(season_id, team_id, player_id) DO UPDATE SET
          sweaterNumber=COALESCE(excluded.sweaterNumber, sweaterNumber),
          positionCode =COALESCE(excluded.positionCode, positionCode)
    """,
        roster_rows,
    )
    conn.commit()


def upsert_team_from_pbp(conn: sqlite3.Connection, team_data: dict[str, Any]) -> None:
    if not (team_id := team_data.get("id")):
        return

    full_name = team_data.get("commonName", {}).get("default")
    abbrev = team_data.get("abbrev")

    if not full_name and not abbrev:
        ensure_team(conn, team_id)
        return

    with conn:
        conn.execute(
            """
            INSERT INTO Team(team_id, fullName, triCode) VALUES (?, ?, ?)
            ON CONFLICT(team_id) DO UPDATE SET
              fullName = COALESCE(excluded.fullName, fullName),
              triCode = COALESCE(excluded.triCode, triCode);
            """,
            (team_id, full_name, abbrev),
        )


def upsert_games_basic(conn: sqlite3.Connection, games: list[dict[str, Any]]) -> None:
    sql = """
    INSERT INTO Game(
        game_id, season_id, gameDate, gameType, gameNumber, gameScheduleStateId,
        gameStateId, period, homeTeamId, awayTeamId, homeScore, awayScore
    )
    VALUES (
        :id, :season, :gameDate, :gameType, :gameNumber, :gameScheduleStateId,
        :gameStateId, :period, :homeTeamId, :visitingTeamId, :homeScore, :visitingScore
    )
    ON CONFLICT(game_id) DO UPDATE SET
        season_id=excluded.season_id,
        gameDate=excluded.gameDate,
        gameType=excluded.gameType,
        gameNumber=excluded.gameNumber,
        gameScheduleStateId=excluded.gameScheduleStateId,
        gameStateId=excluded.gameStateId,
        period=excluded.period,
        homeTeamId=excluded.homeTeamId,
        awayTeamId=excluded.awayTeamId,
        homeScore=excluded.homeScore,
        awayScore=excluded.awayScore;
    """
    # Assurer FK Season + Teams AVANT l'insert
    for g in games:
        ensure_season(conn, g.get("season"))
        ensure_team(conn, g.get("homeTeamId"))
        ensure_team(conn, g.get("visitingTeamId"))

    with conn:
        conn.executemany(sql, games)


# Helper for default values in player fields
def _get_default(d: dict | None) -> Any:
    if not d:
        return None
    val = d.get("default") if isinstance(d, dict) else None
    return val


def upsert_players_from_roster(
        conn: sqlite3.Connection, season_id: int, team_tricode: str, team_id: int
) -> None:
    """Fetch a team roster for a season and upsert players + roster mapping.
    Uses API: https://api-web.nhle.com/v1/roster/<tricode>/<season_id>
    """
    if not team_tricode or not season_id:
        return
    players_rows, roster_rows = build_roster_rows(season_id, team_tricode, team_id)
    if not players_rows:
        return

    sql_players = """
    INSERT INTO Player(
      player_id, firstName, lastName, headshot, shootsCatches, positionCode,
      birthDate, birthCity, birthStateProvince, birthCountry,
      heightInInches, weightInPounds, heightInCentimeters, weightInKilograms
    ) VALUES (
      :player_id, :firstName, :lastName, :headshot, :shootsCatches, :positionCode,
      :birthDate, :birthCity, :birthStateProvince, :birthCountry,
      :heightInInches, :weightInPounds, :heightInCentimeters, :weightInKilograms
    )
    ON CONFLICT(player_id) DO UPDATE SET
      firstName = COALESCE(excluded.firstName, firstName),
      lastName  = COALESCE(excluded.lastName, lastName),
      headshot  = COALESCE(excluded.headshot, headshot),
      shootsCatches = COALESCE(excluded.shootsCatches, shootsCatches),
      positionCode  = COALESCE(excluded.positionCode, positionCode),
      birthDate = COALESCE(excluded.birthDate, birthDate),
      birthCity = COALESCE(excluded.birthCity, birthCity),
      birthStateProvince = COALESCE(excluded.birthStateProvince, birthStateProvince),
      birthCountry = COALESCE(excluded.birthCountry, birthCountry),
      heightInInches = COALESCE(excluded.heightInInches, heightInInches),
      weightInPounds = COALESCE(excluded.weightInPounds, weightInPounds),
      heightInCentimeters = COALESCE(excluded.heightInCentimeters, heightInCentimeters),
      weightInKilograms  = COALESCE(excluded.weightInKilograms, weightInKilograms);
    """

    sql_roster = """
    INSERT INTO Roster(season_id, team_id, player_id, sweaterNumber, positionCode)
    VALUES (:season_id, :team_id, :player_id, :sweaterNumber, :positionCode)
    ON CONFLICT(season_id, team_id, player_id) DO UPDATE SET
      sweaterNumber = COALESCE(excluded.sweaterNumber, sweaterNumber),
      positionCode  = COALESCE(excluded.positionCode, positionCode);
    """

    with conn:
        conn.executemany(sql_players, players_rows)
        conn.executemany(sql_roster, roster_rows)
    return


def upsert_events_for_game(conn: sqlite3.Connection, game_id: int) -> None:
    payload = build_pbp_rows(game_id)
    meta = payload.get("meta", {}) or {}
    teams = payload.get("teams", []) or []
    events = payload.get("events", []) or []
    roster_spots = payload.get("rosterSpots", []) or []

    # verify if roster_spots is empty if yes raise an error
    if not roster_spots:
        raise RuntimeError(
            f"Roster spots missing in PBP for game {game_id}. Cannot proceed."
        )
    
    with conn:  # 1) single transaction
        # a) upsert teams coming from PBP payload
        for t in teams:
            if t:
                upsert_team_from_pbp(conn, t)

        # b) ensure we know season & team ids; if game not present yet, bail early
        row = conn.execute(
            "SELECT season_id, homeTeamId, awayTeamId FROM Game WHERE game_id=?",
            (game_id,),
        ).fetchone()
        # turn tuple row into dict with keys values
        row = (
            {k: v for k, v in zip(("season_id", "homeTeamId", "awayTeamId"), row)}
            if row
            else None
        )

        if not row:
            # You can also choose to insert a stub Game here if needed.
            # For now, fail fast so caller can create the Game first.
            raise RuntimeError(
                f"Game {game_id} not found in DB. Insert the Game before events."
            )

        season_id = row["season_id"]
        home_id = row["homeTeamId"]
        away_id = row["awayTeamId"]

        # c) ensure rosters for both teams of this game (this fixes missing names later)
        ensure_team_roster(conn, season_id, home_id)
        ensure_team_roster(conn, season_id, away_id)

        if roster_spots:
            upsert_players_from_pbp_roster(conn, season_id, roster_spots)

        # d) update Game meta + hasPlays
        conn.execute(
            """
            UPDATE Game
               SET startTimeUTC  = COALESCE(?, startTimeUTC),
                   venue         = COALESCE(?, venue),
                   venueLocation = COALESCE(?, venueLocation),
                   hasPlays      = ?
             WHERE game_id = ?;
            """,
            (
                meta.get("startTimeUTC"),
                meta.get("venue"),
                meta.get("venueLocation"),
                int(bool(meta.get("hasPlays", 0))),  # normalize to 0/1
                game_id,
            ),
        )

        if not events:
            return
        
        # e) normalize emptyNet to 0/1 at insert time (optional but cleaner)
        for ev in events:
            ev["emptyNet"] = 1 if ev.get("emptyNet") else 0
        
        # f) upsert Events
        sql = """
        INSERT INTO Event(
          game_id, eventId, period, periodType, timeInPeriod, timeRemaining,
          typeCode, typeDescKey, sortOrder,
          penaltyTypeCode, penaltyDuration, committedByPlayerId, eventOwnerTeamId,
          details_json, xCoord, yCoord, shotType, shootingPlayerId, goalieInNetId, zoneCode, emptyNet,
          situationCode, homeTeamDefendingSide
        )
        VALUES (
          :game_id, :eventId, :period, :periodType, :timeInPeriod, :timeRemaining,
          :typeCode, :typeDescKey, :sortOrder,
          :penaltyTypeCode, :penaltyDuration, :committedByPlayerId, :eventOwnerTeamId,
          :details_json, :xCoord, :yCoord, :shotType, :shootingPlayerId, :goalieInNetId, :zoneCode, :emptyNet,
          :situationCode, :homeTeamDefendingSide
        )
        ON CONFLICT(game_id, eventId) DO UPDATE SET
          period             = excluded.period,
          periodType         = excluded.periodType,
          timeInPeriod       = excluded.timeInPeriod,
          timeRemaining      = excluded.timeRemaining,
          typeCode           = excluded.typeCode,
          typeDescKey        = excluded.typeDescKey,
          sortOrder          = excluded.sortOrder,
          penaltyTypeCode    = excluded.penaltyTypeCode,
          penaltyDuration    = excluded.penaltyDuration,
          committedByPlayerId= excluded.committedByPlayerId,
          eventOwnerTeamId   = excluded.eventOwnerTeamId,
          details_json       = excluded.details_json,
          xCoord             = excluded.xCoord,
          yCoord             = excluded.yCoord,
          shotType           = excluded.shotType,
          shootingPlayerId   = excluded.shootingPlayerId,
          goalieInNetId      = excluded.goalieInNetId,
          zoneCode           = excluded.zoneCode,
          emptyNet           = excluded.emptyNet,
          situationCode      = COALESCE(excluded.situationCode, situationCode),
          homeTeamDefendingSide  = COALESCE(excluded.homeTeamDefendingSide, homeTeamDefendingSide);
        """
        conn.executemany(sql, events)

def repair_allstar_teams(conn: sqlite3.Connection) -> None:
    cur = conn.execute("SELECT 1 FROM Team WHERE fullName IS NULL LIMIT 1;")
    if not cur.fetchone():
        return  # Nothing to repair

    cur = conn.execute("SELECT game_id FROM Game where gameType = 4;")
    allstar_games = [row[0] for row in cur.fetchall()]

    for game_id in allstar_games:
        data = get_json(API_PBP.format(game_id=game_id))
        for team in [data.get("homeTeam"), data.get("awayTeam")]:
            if team:
                upsert_team_from_pbp(conn, team)


def refresh_hasplays(conn: sqlite3.Connection) -> None:
    """
    Recompute hasPlays for each game by checking if events exist.
    """
    cur = conn.execute("SELECT game_id FROM Game;")
    game_ids = [row[0] for row in cur.fetchall()]
    for gid in game_ids:
        has_events = game_has_events(conn, gid)
        with conn:
            conn.execute(
                "UPDATE Game SET hasPlays = ? WHERE game_id = ?",
                (1 if has_events else 0, gid),
            )


def init_DB():
    conn = sqlite3.Connection(DB_PATH)
    pbar = None
    try:
        init_db(conn)

        # Single dynamic progress bar
        base_steps = 1  # seasons
        base_steps += 1  # teams
        base_steps += 1  # players
        base_steps += len(SEASONS)  # per-season games fetch
        pbar = tqdm(
            total=base_steps, desc="Ingestion NHL", unit="étape", dynamic_ncols=True
        )
        # 1) SAISONS (skip si déjà rempli)
        if table_has_rows(conn, "Season"):
            pbar.update(1)
        else:
            seasons_all = get_seasons()
            seasons = [s for s in seasons_all if s.get("id") in SEASONS]
            upsert_seasons(conn, seasons)
            pbar.update(1)

        # 2) ÉQUIPES (skip si déjà rempli)
        if table_has_rows(conn, "Team"):
            pbar.update(1)
        else:
            teams = get_teams()
            upsert_teams(conn, teams)
            pbar.update(1)
        
        # 3) JOUEURS — uniquement saisons 2016-2017 à 2023-2024
        if table_has_rows(conn, "Player"):
            pbar.update(1)
        else:
            # Récupérer toutes les équipes (id + tricode) depuis la table Team
            cur = conn.execute(
                "SELECT team_id, COALESCE(rawTricode, triCode) FROM Team WHERE rawTricode IS NOT NULL OR triCode IS NOT NULL;"
            )
            teams_rows = [(row[0], row[1]) for row in cur.fetchall() if row[1]]
            # Ingestion par saison × équipe
            for season in SEASONS:
                for team_id, tricode in teams_rows:
                    try:
                        upsert_players_from_roster(conn, season, tricode, team_id)
                    except Exception:
                        # on continue en cas d'erreur réseau ponctuelle
                        continue
            pbar.update(1)

        # 4) MATCHS — uniquement saisons 2016-2017 à 2023-2024
        all_games_in_range: list[dict[str, Any]] = []
        existing_game_ids = existing_ids(conn, "Game", "game_id")
        for season in SEASONS:
            games = get_games_for_season(season)
            new_games = [g for g in games if g.get("id") not in existing_game_ids]
            if new_games:
                upsert_games_basic(conn, new_games)
                existing_game_ids.update(
                    g["id"] for g in new_games if g.get("id") is not None
                )
            all_games_in_range.extend(games)
            pbar.update(1)

        # 5) ÉVÉNEMENTS — seulement pour les matchs des saisons ciblées, et uniquement ceux non traités
        to_process = [
            int(g["id"])
            for g in all_games_in_range
            if g.get("id") and not game_has_events(conn, int(g["id"]))
        ]

        # Extend the single bar's total to account for per-game PBP ingestion
        pbar.total = pbar.total + len(to_process)
        pbar.refresh()

        for gid in to_process:
            row = conn.execute(
                "SELECT hasPlays FROM Game WHERE game_id=?;", (gid,)
            ).fetchone()
            # Ne faire l'upsert des événements que si hasPlays == 1
            if row is None or row[0] is None or row[0] == 1:
                try:
                    upsert_events_for_game(conn, gid)
                except Exception as e:
                    # If game data not available (404, etc), mark as hasPlays=0
                    if "404" in str(e):
                        with conn:
                            conn.execute(
                                "UPDATE Game SET hasPlays=0 WHERE game_id=?;", (gid,)
                            )
                        # Continue to next game regardless of error
                        pass
            # Sinon, on ne fait rien pour ce match
            pbar.update(1)

        pbar.close() if pbar is not None else None

        # Reparer matchs des etoiles
        repair_allstar_teams(conn)

        # Backfill shot columns from JSON if needed
        backfill_shot_columns_if_needed(conn)

        # refresh_hasplays(conn)
        print("Ingestion terminée.")

    finally:
        if pbar is not None:
            try:
                pbar.close()
            except Exception:
                pass
        conn.close()


if __name__ == "__main__":
    init_DB()
