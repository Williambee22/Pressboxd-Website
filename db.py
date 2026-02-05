import sqlite3
import time
import re
from typing import Any, Dict, List, Optional, Tuple

def now_ts() -> int:
    return int(time.time())

def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    return conn

def init_db(db_path: str, schema_path: str = "schema.sql") -> None:
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = f.read()
    conn = connect(db_path)
    try:
        conn.executescript(schema)
        conn.commit()
    finally:
        conn.close()

def norm_key(year: int, corps: str, title: str) -> str:
    # Stable uniqueness key: "2017|blue devils|metamorph"
    def norm(s: str) -> str:
        s = s.strip().lower()
        s = re.sub(r"\s+", " ", s)
        s = re.sub(r"[^a-z0-9 \-]", "", s)
        return s.strip()
    return f"{int(year)}|{norm(corps)}|{norm(title)}"

def is_first_user(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()
    return int(row["n"]) == 0

def user_by_username(conn: sqlite3.Connection, username: str):
    return conn.execute("SELECT * FROM users WHERE username = ?", (username.strip(),)).fetchone()

def user_by_id(conn: sqlite3.Connection, user_id: int):
    return conn.execute("SELECT * FROM users WHERE id = ?", (int(user_id),)).fetchone()

def create_user(conn: sqlite3.Connection, username: str, pass_hash: str, make_admin: bool = False) -> int:
    conn.execute(
        "INSERT INTO users(username, pass_hash, is_admin, created_ts) VALUES (?,?,?,?)",
        (username.strip(), pass_hash, 1 if make_admin else 0, now_ts()),
    )
    conn.commit()
    return int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])

def add_show(conn: sqlite3.Connection, year: int, corps: str, title: str, poster_url: Optional[str] = None) -> Tuple[bool, Optional[int], str]:
    year_i = int(year)
    corps_s = corps.strip()
    title_s = title.strip()
    poster_s = (poster_url or "").strip() or None

    if not corps_s or not title_s:
        return (False, None, "Missing corps or title.")

    key = norm_key(year_i, corps_s, title_s)

    try:
        conn.execute(
            "INSERT INTO shows(title, corps, year, poster_url, norm_key, created_ts) VALUES (?,?,?,?,?,?)",
            (title_s, corps_s, year_i, poster_s, key, now_ts()),
        )
        conn.commit()
        show_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
        return (True, show_id, "Inserted")

    except sqlite3.IntegrityError:
        # Duplicate show — optionally update poster_url if you provided one and DB is empty
        row = conn.execute("SELECT id, poster_url FROM shows WHERE norm_key = ?", (key,)).fetchone()
        if row and poster_s and (row["poster_url"] is None or str(row["poster_url"]).strip() == ""):
            conn.execute("UPDATE shows SET poster_url = ? WHERE id = ?", (poster_s, int(row["id"])))
            conn.commit()
            return (False, int(row["id"]), "Duplicate (poster updated)")
        return (False, int(row["id"]) if row else None, "Duplicate")

def upsert_rating(conn: sqlite3.Connection, show_id: int, user_id: int, rating_half: int) -> None:
    rh = max(0, min(10, int(rating_half)))  # 0..10
    conn.execute(
        """
        INSERT INTO ratings(show_id, user_id, rating_half, ts)
        VALUES (?,?,?,?)
        ON CONFLICT(show_id, user_id) DO UPDATE SET
          rating_half=excluded.rating_half,
          ts=excluded.ts
        """,
        (int(show_id), int(user_id), rh, now_ts()),
    )
    conn.commit()

def ensure_half_star_migration(conn: sqlite3.Connection) -> None:
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(ratings)").fetchall()]
    if "rating_half" not in cols:
        conn.execute("ALTER TABLE ratings ADD COLUMN rating_half INTEGER NOT NULL DEFAULT 0;")
        # If old column exists, migrate values
        if "rating_int" in cols:
            conn.execute("UPDATE ratings SET rating_half = COALESCE(rating_int, 0) * 2;")
        conn.commit()



def delete_rating(conn: sqlite3.Connection, show_id: int, user_id: int) -> None:
    conn.execute("DELETE FROM ratings WHERE show_id=? AND user_id=?", (int(show_id), int(user_id)))
    conn.commit()

def list_shows(
    conn: sqlite3.Connection,
    sort: str = "year",
    year_filter: Optional[int] = None,
    corps_filter: Optional[str] = None,
) -> List[sqlite3.Row]:
    # Aggregates for average rating and rating count
    # We keep unrated shows last for avg-based sorts.
    where = []
    params: List[Any] = []
    if year_filter is not None:
        where.append("s.year = ?")
        params.append(int(year_filter))
    if corps_filter:
        where.append("LOWER(s.corps) = LOWER(?)")
        params.append(corps_filter.strip())

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    # sort options:
    # year -> year desc, corps asc, title asc
    # corps -> corps asc, year desc, title asc
    # top -> avg desc, count desc, year desc
    # bottom -> avg asc, count desc, year desc (but unrated last)
    if sort == "corps":
        order_sql = "ORDER BY LOWER(s.corps) ASC, s.year DESC, LOWER(s.title) ASC"
    elif sort == "top":
        order_sql = """
        ORDER BY
        CASE WHEN cnt = 0 THEN 1 ELSE 0 END ASC,
        avg_rating DESC,
        cnt DESC,
        s.year DESC,
        LOWER(s.corps) ASC,
        LOWER(s.title) ASC
        """
    elif sort == "bottom":
        order_sql = """
        ORDER BY
        CASE WHEN cnt = 0 THEN 1 ELSE 0 END ASC,
        avg_rating ASC,
        cnt DESC,
        s.year DESC,
        LOWER(s.corps) ASC,
        LOWER(s.title) ASC
        """
    elif sort == "year_asc":
        order_sql = "ORDER BY s.year ASC, LOWER(s.corps) ASC, LOWER(s.title) ASC"
    else:  # default year_desc
        order_sql = "ORDER BY s.year DESC, LOWER(s.corps) ASC, LOWER(s.title) ASC"

    q = f"""
    SELECT
      s.*,
        (COALESCE(AVG(r.rating_half), 0.0) / 2.0) AS avg_rating,
        COUNT(r.rating_half) AS cnt
    FROM shows s
    LEFT JOIN ratings r ON r.show_id = s.id
    {where_sql}
    GROUP BY s.id
    {order_sql}
    """
    return conn.execute(q, params).fetchall()

def show_detail(conn: sqlite3.Connection, show_id: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
          s.*,
          (COALESCE(AVG(r.rating_half), 0.0) / 2.0) AS avg_rating,
          COUNT(r.rating_half) AS cnt
        FROM shows s
        LEFT JOIN ratings r ON r.show_id = s.id
        WHERE s.id = ?
        GROUP BY s.id
        """,
        (int(show_id),),
    ).fetchone()

def user_rating_for_show(conn: sqlite3.Connection, show_id: int, user_id: int) -> Optional[int]:
    row = conn.execute(
        "SELECT rating_half FROM ratings WHERE show_id=? AND user_id=?",
        (int(show_id), int(user_id)),
    ).fetchone()
    return int(row["rating_half"]) if row else None


def get_show_by_id(conn: sqlite3.Connection, show_id: int) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM shows WHERE id = ?", (int(show_id),)).fetchone()


def ensure_poster_url_column(conn: sqlite3.Connection) -> None:
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(shows)").fetchall()]
    if "poster_url" not in cols:
        conn.execute("ALTER TABLE shows ADD COLUMN poster_url TEXT;")
        conn.commit()

def update_show(
    conn: sqlite3.Connection,
    show_id: int,
    year: int,
    corps: str,
    title: str,
    poster_url: Optional[str] = None,
) -> None:
    year_i = int(year)
    corps_s = corps.strip()
    title_s = title.strip()
    poster_s = (poster_url or "").strip() or None

    if not corps_s or not title_s:
        raise ValueError("Missing corps or title.")

    new_key = norm_key(year_i, corps_s, title_s)

    # Prevent collision with an existing show's norm_key (other than this show)
    row = conn.execute(
        "SELECT id FROM shows WHERE norm_key = ? AND id <> ?",
        (new_key, int(show_id)),
    ).fetchone()
    if row:
        raise ValueError("Another show already exists with the same year/corps/title (duplicate).")

    conn.execute(
        """
        UPDATE shows
        SET title = ?, corps = ?, year = ?, poster_url = ?, norm_key = ?
        WHERE id = ?
        """,
        (title_s, corps_s, year_i, poster_s, new_key, int(show_id)),
    )
    conn.commit()

def upsert_review(conn: sqlite3.Connection, show_id: int, user_id: int, review_text: str) -> None:
    text = (review_text or "").strip()
    if not text:
        raise ValueError("Review cannot be empty.")
    if len(text) > 5000:
        raise ValueError("Review is too long (max 5000 chars).")

    conn.execute(
        """
        INSERT INTO reviews(show_id, user_id, review_text, ts)
        VALUES (?,?,?,?)
        ON CONFLICT(show_id, user_id) DO UPDATE SET
          review_text=excluded.review_text,
          ts=excluded.ts
        """,
        (int(show_id), int(user_id), text, now_ts()),
    )
    conn.commit()


def delete_review(conn: sqlite3.Connection, show_id: int, user_id: int) -> None:
    conn.execute("DELETE FROM reviews WHERE show_id=? AND user_id=?", (int(show_id), int(user_id)))
    conn.commit()


def my_review_for_show(conn: sqlite3.Connection, show_id: int, user_id: int) -> Optional[str]:
    row = conn.execute(
        "SELECT review_text FROM reviews WHERE show_id=? AND user_id=?",
        (int(show_id), int(user_id)),
    ).fetchone()
    return str(row["review_text"]) if row else None


def list_reviews_for_show(conn, show_id: int, viewer_user_id=None, limit: int = 30):
    viewer_user_id = int(viewer_user_id) if viewer_user_id is not None else None

    return conn.execute(
        """
        SELECT
          rv.review_text, rv.ts,
          u.id AS user_id, u.username, u.avatar_url,
          r.rating_half,

          -- primary role (first assigned)
          (
            SELECT rl.name
            FROM user_roles ur
            JOIN roles rl ON rl.id = ur.role_id
            WHERE ur.user_id = u.id
            ORDER BY ur.assigned_ts ASC
            LIMIT 1
          ) AS role_name,
          (
            SELECT rl.color
            FROM user_roles ur
            JOIN roles rl ON rl.id = ur.role_id
            WHERE ur.user_id = u.id
            ORDER BY ur.assigned_ts ASC
            LIMIT 1
          ) AS role_color,

          -- vote score for this review (up - down)
          COALESCE((
            SELECT SUM(v.vote)
            FROM review_votes v
            WHERE v.show_id = rv.show_id AND v.review_user_id = rv.user_id
          ), 0) AS vote_score,

          -- the viewer's vote on this review (+1/-1/NULL)
          (
            SELECT v.vote
            FROM review_votes v
            WHERE v.show_id = rv.show_id
              AND v.review_user_id = rv.user_id
              AND v.voter_user_id = ?
          ) AS my_vote

        FROM reviews rv
        JOIN users u ON u.id = rv.user_id
        LEFT JOIN ratings r ON r.show_id = rv.show_id AND r.user_id = rv.user_id
        WHERE rv.show_id = ?
        ORDER BY rv.ts DESC
        LIMIT ?
        """,
        (viewer_user_id, int(show_id), int(limit)),
    ).fetchall()

def recent_ratings_for_user(conn: sqlite3.Connection, user_id: int, limit: int = 24) -> List[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
          s.id AS show_id, s.title, s.corps, s.year, s.poster_url,
          r.rating_half, r.ts
        FROM ratings r
        JOIN shows s ON s.id = r.show_id
        WHERE r.user_id = ?
        ORDER BY r.ts DESC
        LIMIT ?
        """,
        (int(user_id), int(limit)),
    ).fetchall()


def recent_reviews_for_user(conn: sqlite3.Connection, user_id: int, limit: int = 12) -> List[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
          s.id AS show_id, s.title, s.corps, s.year, s.poster_url,
          rv.review_text, rv.ts,
          r.rating_half
        FROM reviews rv
        JOIN shows s ON s.id = rv.show_id
        LEFT JOIN ratings r ON r.show_id = rv.show_id AND r.user_id = rv.user_id
        WHERE rv.user_id = ?
        ORDER BY rv.ts DESC
        LIMIT ?
        """,
        (int(user_id), int(limit)),
    ).fetchall()



def ensure_reviews_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS reviews (
          show_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL,
          review_text TEXT NOT NULL,
          ts INTEGER NOT NULL,
          PRIMARY KEY(show_id, user_id),
          FOREIGN KEY(show_id) REFERENCES shows(id) ON DELETE CASCADE,
          FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_reviews_show_id ON reviews(show_id);
        CREATE INDEX IF NOT EXISTS idx_reviews_user_id ON reviews(user_id);
        CREATE INDEX IF NOT EXISTS idx_reviews_ts ON reviews(ts);
        """
    )
    conn.commit()


def ensure_profile_columns(conn: sqlite3.Connection) -> None:
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(users)").fetchall()]
    if "avatar_url" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN avatar_url TEXT;")
    if "banner_url" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN banner_url TEXT;")
    if "theme_color" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN theme_color TEXT;")
    conn.commit()


_HEX_COLOR_RE = re.compile(r"^#(?:[0-9a-fA-F]{6})$")

def _clean_color(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    return s if _HEX_COLOR_RE.match(s) else ""

def update_user_profile_style(
    conn: sqlite3.Connection,
    user_id: int,
    avatar_url: str,
    banner_url: str,
    theme_color: str,
) -> None:
    a = (avatar_url or "").strip() or None
    b = (banner_url or "").strip() or None
    c = _clean_color(theme_color) or None

    conn.execute(
        """
        UPDATE users
        SET avatar_url = ?, banner_url = ?, theme_color = ?
        WHERE id = ?
        """,
        (a, b, c, int(user_id)),
    )
    conn.commit()


_HEX_COLOR_RE = re.compile(r"^#(?:[0-9a-fA-F]{6})$")

def _slugify(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "role"

def _clean_hex_color(s: str) -> str:
    s = (s or "").strip()
    return s if _HEX_COLOR_RE.match(s) else ""


def ensure_roles_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS roles (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL UNIQUE,
          slug TEXT NOT NULL UNIQUE,
          color TEXT,
          created_ts INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS user_roles (
          user_id INTEGER NOT NULL,
          role_id INTEGER NOT NULL,
          assigned_ts INTEGER NOT NULL,
          assigned_by_user_id INTEGER,
          PRIMARY KEY(user_id, role_id),
          FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
          FOREIGN KEY(role_id) REFERENCES roles(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_user_roles_user_id ON user_roles(user_id);
        CREATE INDEX IF NOT EXISTS idx_user_roles_role_id ON user_roles(role_id);
        """
    )
    conn.commit()


def list_roles(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM roles ORDER BY name ASC").fetchall()


def create_role(conn: sqlite3.Connection, name: str, color: str = "") -> None:
    nm = (name or "").strip()
    if not nm:
        raise ValueError("Role name is required.")

    slug = _slugify(nm)
    col = _clean_hex_color(color) or None

    # Ensure unique slug; if collision, add -2, -3, ...
    base = slug
    n = 2
    while conn.execute("SELECT 1 FROM roles WHERE slug = ?", (slug,)).fetchone():
        slug = f"{base}-{n}"
        n += 1

    conn.execute(
        "INSERT INTO roles(name, slug, color, created_ts) VALUES (?,?,?,?)",
        (nm, slug, col, now_ts()),
    )
    conn.commit()


def delete_role(conn: sqlite3.Connection, role_id: int) -> None:
    conn.execute("DELETE FROM roles WHERE id = ?", (int(role_id),))
    conn.commit()


def assign_role_to_user(conn: sqlite3.Connection, user_id: int, role_id: int, assigned_by_user_id: int | None) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO user_roles(user_id, role_id, assigned_ts, assigned_by_user_id)
        VALUES (?,?,?,?)
        """,
        (int(user_id), int(role_id), now_ts(), int(assigned_by_user_id) if assigned_by_user_id is not None else None),
    )
    conn.commit()


def remove_role_from_user(conn: sqlite3.Connection, user_id: int, role_id: int) -> None:
    conn.execute(
        "DELETE FROM user_roles WHERE user_id = ? AND role_id = ?",
        (int(user_id), int(role_id)),
    )
    conn.commit()


def roles_for_user(conn: sqlite3.Connection, user_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT r.*
        FROM user_roles ur
        JOIN roles r ON r.id = ur.role_id
        WHERE ur.user_id = ?
        ORDER BY ur.assigned_ts ASC
        """,
        (int(user_id),),
    ).fetchall()


def primary_role_for_user(conn: sqlite3.Connection, user_id: int) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT r.*
        FROM user_roles ur
        JOIN roles r ON r.id = ur.role_id
        WHERE ur.user_id = ?
        ORDER BY ur.assigned_ts ASC
        LIMIT 1
        """,
        (int(user_id),),
    ).fetchone()


def role_by_id(conn: sqlite3.Connection, role_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM roles WHERE id = ?", (int(role_id),)).fetchone()


def update_role(conn: sqlite3.Connection, role_id: int, name: str, color: str = "") -> None:
    nm = (name or "").strip()
    if not nm:
        raise ValueError("Role name is required.")

    col = _clean_hex_color(color) or None  # uses your existing validator

    conn.execute(
        """
        UPDATE roles
        SET name = ?, color = ?
        WHERE id = ?
        """,
        (nm, col, int(role_id)),
    )
    conn.commit()



def ensure_review_votes_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS review_votes (
          show_id INTEGER NOT NULL,
          review_user_id INTEGER NOT NULL,
          voter_user_id INTEGER NOT NULL,
          vote INTEGER NOT NULL,
          ts INTEGER NOT NULL,
          PRIMARY KEY (show_id, review_user_id, voter_user_id),
          FOREIGN KEY (show_id) REFERENCES shows(id) ON DELETE CASCADE,
          FOREIGN KEY (review_user_id) REFERENCES users(id) ON DELETE CASCADE,
          FOREIGN KEY (voter_user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_review_votes_show ON review_votes(show_id);
        CREATE INDEX IF NOT EXISTS idx_review_votes_review_user ON review_votes(review_user_id);
        CREATE INDEX IF NOT EXISTS idx_review_votes_voter_user ON review_votes(voter_user_id);
        """
    )
    conn.commit()


def set_review_vote(conn: sqlite3.Connection, show_id: int, review_user_id: int, voter_user_id: int, vote: int) -> None:
    """
    vote: +1 (upvote) or -1 (downvote)
    Toggle behavior:
      - If same vote already exists -> remove vote
      - If opposite vote exists -> switch to new vote
      - If none -> insert
    """
    show_id = int(show_id)
    review_user_id = int(review_user_id)
    voter_user_id = int(voter_user_id)
    vote = 1 if int(vote) > 0 else -1

    cur = conn.execute(
        """
        SELECT vote
        FROM review_votes
        WHERE show_id = ? AND review_user_id = ? AND voter_user_id = ?
        """,
        (show_id, review_user_id, voter_user_id),
    ).fetchone()

    if cur is None:
        conn.execute(
            """
            INSERT INTO review_votes(show_id, review_user_id, voter_user_id, vote, ts)
            VALUES (?,?,?,?,?)
            """,
            (show_id, review_user_id, voter_user_id, vote, now_ts()),
        )
    else:
        existing = int(cur["vote"])
        if existing == vote:
            conn.execute(
                """
                DELETE FROM review_votes
                WHERE show_id = ? AND review_user_id = ? AND voter_user_id = ?
                """,
                (show_id, review_user_id, voter_user_id),
            )
        else:
            conn.execute(
                """
                UPDATE review_votes
                SET vote = ?, ts = ?
                WHERE show_id = ? AND review_user_id = ? AND voter_user_id = ?
                """,
                (vote, now_ts(), show_id, review_user_id, voter_user_id),
            )

    conn.commit()



def list_top_shows_with_top_review(conn, limit: int = 25, mode: str = "top"):
    limit = int(limit)
    mode = (mode or "top").strip().lower()

    # Top: highest avg first; Bottom: lowest avg first
    if mode == "bottom":
        order_sql = "ss.avg_rating ASC, ss.cnt DESC, ss.year DESC, ss.id ASC"
    else:
        order_sql = "ss.avg_rating DESC, ss.cnt DESC, ss.year DESC, ss.id ASC"

    sql = f"""
        WITH show_stats AS (
          SELECT
            s.id,
            s.title,
            s.corps,
            s.year,
            s.poster_url,
            COUNT(r.rating_half) AS cnt,
            COALESCE(ROUND(AVG(r.rating_half) / 2.0, 2), 0) AS avg_rating
          FROM shows s
          LEFT JOIN ratings r ON r.show_id = s.id
          GROUP BY s.id
        ),

        review_scores AS (
          SELECT
            rv.show_id,
            rv.user_id,
            rv.review_text,
            rv.ts,
            u.username,
            u.avatar_url,
            r.rating_half AS review_rating_half,
            COALESCE((
              SELECT SUM(v.vote)
              FROM review_votes v
              WHERE v.show_id = rv.show_id AND v.review_user_id = rv.user_id
            ), 0) AS vote_score
          FROM reviews rv
          JOIN users u ON u.id = rv.user_id
          LEFT JOIN ratings r ON r.show_id = rv.show_id AND r.user_id = rv.user_id
        ),

        top_review_per_show AS (
          SELECT
            rs.*,
            ROW_NUMBER() OVER (
              PARTITION BY rs.show_id
              ORDER BY rs.vote_score DESC, rs.ts DESC
            ) AS rn
          FROM review_scores rs
        )

        SELECT
          ss.*,
          tr.username AS top_review_username,
          tr.avatar_url AS top_review_avatar_url,
          tr.review_text AS top_review_text,
          tr.vote_score AS top_review_score,
          tr.ts AS top_review_ts,
          tr.review_rating_half AS top_review_rating_half
        FROM show_stats ss
        LEFT JOIN top_review_per_show tr
          ON tr.show_id = ss.id AND tr.rn = 1
        WHERE ss.cnt > 0
        ORDER BY {order_sql}
        LIMIT ?
    """

    return conn.execute(sql, (limit,)).fetchall()