PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT NOT NULL UNIQUE,
  pass_hash TEXT NOT NULL,
  is_admin INTEGER NOT NULL DEFAULT 0,
  created_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS shows (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  corps TEXT NOT NULL,
  year INTEGER NOT NULL,
  norm_key TEXT NOT NULL UNIQUE,
  created_ts INTEGER NOT NULL
  poster_url TEXT,
);

CREATE INDEX IF NOT EXISTS idx_shows_year  ON shows(year);
CREATE INDEX IF NOT EXISTS idx_shows_corps ON shows(corps);

CREATE TABLE IF NOT EXISTS ratings (
  show_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  rating_half INTEGER NOT NULL, -- 0..10 (each step = 0.5 star)
  ts INTEGER NOT NULL,
  PRIMARY KEY (show_id, user_id),
  FOREIGN KEY(show_id) REFERENCES shows(id) ON DELETE CASCADE,
  FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ratings_show_id ON ratings(show_id);
CREATE INDEX IF NOT EXISTS idx_ratings_user_id ON ratings(user_id);

ALTER TABLE ratings ADD COLUMN rating_half INTEGER NOT NULL DEFAULT 0;
UPDATE ratings SET rating_half = rating_int * 2;

CREATE TABLE IF NOT EXISTS shows (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  corps TEXT NOT NULL,
  year INTEGER NOT NULL,
  poster_url TEXT,              -- NEW
  norm_key TEXT NOT NULL UNIQUE,
  created_ts INTEGER NOT NULL
);

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



CREATE TABLE IF NOT EXISTS review_votes (
  show_id INTEGER NOT NULL,
  review_user_id INTEGER NOT NULL,   -- the author of the review
  voter_user_id INTEGER NOT NULL,    -- the user voting
  vote INTEGER NOT NULL,             -- +1 or -1
  ts INTEGER NOT NULL,
  PRIMARY KEY (show_id, review_user_id, voter_user_id),
  FOREIGN KEY (show_id) REFERENCES shows(id) ON DELETE CASCADE,
  FOREIGN KEY (review_user_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (voter_user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_review_votes_show ON review_votes(show_id);
CREATE INDEX IF NOT EXISTS idx_review_votes_review_user ON review_votes(review_user_id);
CREATE INDEX IF NOT EXISTS idx_review_votes_voter_user ON review_votes(voter_user_id);