import os
from functools import wraps
from typing import Optional, Tuple, List

from flask import Flask, render_template, request, redirect, url_for, session, flash, abort
from werkzeug.security import generate_password_hash, check_password_hash

import db as dbx

APP_SECRET = os.getenv("APP_SECRET", "dev-secret-change-me")
DB_PATH = os.getenv("DC_SITE_DB", os.path.join(os.path.dirname(__file__), "site.db"))

def create_app() -> Flask:
    app = Flask(__name__)
    app.secret_key = APP_SECRET

    # Ensure DB exists
    if not os.path.exists(DB_PATH):
        dbx.init_db(DB_PATH, schema_path=os.path.join(os.path.dirname(__file__), "schema.sql"))

    def get_conn():
        conn = dbx.connect(DB_PATH)
        dbx.ensure_reviews_table(conn)
        dbx.ensure_profile_columns(conn)
        dbx.ensure_roles_tables(conn)
        dbx.ensure_review_votes_table(conn)  # NEW
        return conn

    def current_user():
        uid = session.get("uid")
        if not uid:
            return None
        conn = get_conn()
        try:
            return dbx.user_by_id(conn, int(uid))
        finally:
            conn.close()

    def login_required(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not session.get("uid"):
                flash("Please log in to do that.", "warn")
                return redirect(url_for("login", next=request.path))
            return fn(*args, **kwargs)
        return wrapper

    def admin_required(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            u = current_user()
            if not u or int(u["is_admin"]) != 1:
                abort(403)
            return fn(*args, **kwargs)
        return wrapper

    @app.context_processor
    def inject_globals():
        u = current_user()
        return {"me": u}

    @app.get("/")
    def home():
        return redirect(url_for("shows"))

    @app.get("/shows")
    def shows():
        sort = (request.args.get("sort") or "year_desc").strip().lower()
        if sort not in {"year_desc", "year_asc", "corps", "top", "bottom"}:
            sort = "year_desc"
        year = request.args.get("year")
        corps = request.args.get("corps")

        year_filter = None
        if year and year.isdigit():
            year_filter = int(year)

        conn = get_conn()
        try:
            rows = dbx.list_shows(conn, sort=sort, year_filter=year_filter, corps_filter=corps)
        finally:
            conn.close()

        return render_template("shows.html", rows=rows, sort=sort, year=year_filter, corps=corps)

    @app.get("/show/<int:show_id>")
    def show_detail(show_id: int):
        conn = None
        try:
            conn = get_conn()
            row = dbx.show_detail(conn, show_id)
            if not row:
                abort(404)

            my_rating = None
            my_review = None
            if session.get("uid"):
                my_rating = dbx.user_rating_for_show(conn, show_id, int(session["uid"]))
                my_review = dbx.my_review_for_show(conn, show_id, int(session["uid"]))

            viewer_uid = session.get("uid")
            reviews = dbx.list_reviews_for_show(conn, show_id, viewer_user_id=viewer_uid, limit=30)
        finally:
            if conn is not None:
                conn.close()

        return render_template(
            "show_detail.html",
            row=row,
            my_rating=my_rating,
            my_review=my_review,
            reviews=reviews
        )

    @app.post("/show/<int:show_id>/rate")
    @login_required
    def rate_show(show_id: int):
        rating_half = (request.form.get("rating_half") or "").strip()

        if not rating_half.isdigit():
            flash("Rating must be in half-star steps.", "error")
            return redirect(url_for("show_detail", show_id=show_id))

        rh = int(rating_half)
        if rh < 0 or rh > 10:
            flash("Rating must be between 0 and 5 stars (half steps).", "error")
            return redirect(url_for("show_detail", show_id=show_id))

        conn = get_conn()
        try:
            if not dbx.show_detail(conn, show_id):
                abort(404)
            dbx.upsert_rating(conn, show_id, int(session["uid"]), rh)
        finally:
            conn.close()

        flash("Saved your rating.", "ok")
        return redirect(url_for("show_detail", show_id=show_id))

    @app.post("/show/<int:show_id>/unrate")
    @login_required
    def unrate_show(show_id: int):
        conn = get_conn()
        try:
            dbx.delete_rating(conn, show_id, int(session["uid"]))
        finally:
            conn.close()
        flash("Removed your rating.", "ok")
        return redirect(url_for("show_detail", show_id=show_id))

    @app.get("/register")
    def register():
        return render_template("auth_register.html")

    @app.post("/register")
    def register_post():
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()

        if len(username) < 3:
            flash("Username must be at least 3 characters.", "error")
            return redirect(url_for("register"))
        if len(password) < 6:
            flash("Password must be at least 6 characters.", "error")
            return redirect(url_for("register"))

        conn = get_conn()
        try:
            if dbx.user_by_username(conn, username):
                flash("That username is taken.", "error")
                return redirect(url_for("register"))

            make_admin = dbx.is_first_user(conn)  # first account becomes admin
            uid = dbx.create_user(conn, username, generate_password_hash(password), make_admin=make_admin)
        finally:
            conn.close()

        session["uid"] = uid
        flash("Account created. You're logged in.", "ok")
        if make_admin:
            flash("First user created: you are admin (can mass add shows).", "ok")
        return redirect(url_for("shows"))

    @app.get("/login")
    def login():
        return render_template("auth_login.html", next=request.args.get("next"))

    @app.post("/login")
    def login_post():
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()
        nxt = (request.form.get("next") or "").strip()

        conn = get_conn()
        try:
            u = dbx.user_by_username(conn, username)
        finally:
            conn.close()

        if not u or not check_password_hash(u["pass_hash"], password):
            flash("Invalid username/password.", "error")
            return redirect(url_for("login"))

        session["uid"] = int(u["id"])
        flash("Logged in.", "ok")
        return redirect(nxt or url_for("shows"))

    @app.get("/logout")
    def logout():
        session.pop("uid", None)
        flash("Logged out.", "ok")
        return redirect(url_for("shows"))

    @app.get("/admin/bulk_add")
    @admin_required
    def bulk_add():
        return render_template("bulk_add.html")

    def parse_bulk_lines(text: str):
        """
        Accepts lines like:
        2017, Blue Devils, Metamorph
        2016|Carolina Crown|Relentless
        2017, Blue Devils, Metamorph, https://.../image.jpg
        2017|Blue Devils|Metamorph|https://.../image.jpg

        Fields:
        year, corps, title, [optional poster_url]
        """
        items = []
        errors = []

        for i, raw in enumerate((text or "").splitlines(), start=1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue

            if "|" in line:
                parts = [p.strip() for p in line.split("|")]
            else:
                parts = [p.strip() for p in line.split(",")]

            if len(parts) not in (3, 4):
                errors.append(f"Line {i}: expected 3 or 4 fields (year, corps, title, [poster_url]). Got: {raw}")
                continue

            y, c, t = parts[0], parts[1], parts[2]
            poster = parts[3] if len(parts) == 4 else None

            if not y.isdigit():
                errors.append(f"Line {i}: year must be a number. Got: {y}")
                continue

            items.append((int(y), c, t, poster))

        return items, errors

    @app.post("/admin/bulk_add")
    @admin_required
    def bulk_add_post():
        text = request.form.get("bulk_text") or ""
        items, errors = parse_bulk_lines(text)
        if errors:
            for e in errors[:10]:
                flash(e, "error")
            if len(errors) > 10:
                flash(f"...and {len(errors)-10} more errors.", "error")
            return redirect(url_for("bulk_add"))

        conn = get_conn()
        inserted = 0
        dupes = 0
        other_bad = 0
        try:
            for (y, c, t, poster) in items:
                ok, _sid, msg = dbx.add_show(conn, y, c, t, poster_url=poster)
                if ok:
                    inserted += 1
                else:
                    if msg == "Duplicate":
                        dupes += 1
                    else:
                        other_bad += 1
        finally:
            conn.close()

        flash(f"Bulk add done. Inserted: {inserted}. Duplicates skipped: {dupes}. Other issues: {other_bad}.", "ok")
        return redirect(url_for("shows"))


    @app.get("/admin/show/<int:show_id>/edit")
    @admin_required
    def admin_edit_show(show_id: int):
        conn = get_conn()
        try:
            s = dbx.get_show_by_id(conn, show_id)
            if not s:
                abort(404)
        finally:
            conn.close()

        return render_template("admin_edit_show.html", s=s)


    @app.post("/admin/show/<int:show_id>/edit")
    @admin_required
    def admin_edit_show_post(show_id: int):
        title = (request.form.get("title") or "").strip()
        corps = (request.form.get("corps") or "").strip()
        year  = (request.form.get("year") or "").strip()
        poster_url = (request.form.get("poster_url") or "").strip()

        if not year.isdigit():
            flash("Year must be a number.", "error")
            return redirect(url_for("admin_edit_show", show_id=show_id))

        conn = get_conn()
        try:
            if not dbx.get_show_by_id(conn, show_id):
                abort(404)
            try:
                dbx.update_show(conn, show_id, int(year), corps, title, poster_url=poster_url)
            except ValueError as e:
                flash(str(e), "error")
                return redirect(url_for("admin_edit_show", show_id=show_id))
        finally:
            conn.close()

        flash("Show updated.", "ok")
        return redirect(url_for("show_detail", show_id=show_id))
    

    @app.post("/show/<int:show_id>/review")
    @login_required
    def review_show(show_id: int):
        text = (request.form.get("review_text") or "").strip()

        conn = None
        try:
            conn = get_conn()
            if not dbx.show_detail(conn, show_id):
                abort(404)
            try:
                dbx.upsert_review(conn, show_id, int(session["uid"]), text)
            except ValueError as e:
                flash(str(e), "error")
                return redirect(url_for("show_detail", show_id=show_id))
        finally:
            if conn is not None:
                conn.close()

        flash("Saved your review.", "ok")
        return redirect(url_for("show_detail", show_id=show_id))


    @app.post("/show/<int:show_id>/review/delete")
    @login_required
    def delete_my_review(show_id: int):
        conn = None
        try:
            conn = get_conn()
            dbx.delete_review(conn, show_id, int(session["uid"]))
        finally:
            if conn is not None:
                conn.close()

        flash("Deleted your review.", "ok")
        return redirect(url_for("show_detail", show_id=show_id))

    @app.get("/me")
    @login_required
    def me_profile():
        conn = None
        try:
            conn = get_conn()
            u = dbx.user_by_id(conn, int(session["uid"]))
            if not u:
                abort(404)

            recent_ratings = dbx.recent_ratings_for_user(conn, int(u["id"]), limit=30)
            recent_reviews = dbx.recent_reviews_for_user(conn, int(u["id"]), limit=20)

            # NEW: roles
            user_roles = dbx.roles_for_user(conn, int(u["id"]))
            primary_role = user_roles[0] if user_roles else None

        finally:
            if conn is not None:
                conn.close()

        return render_template(
            "profile.html",
            profile_user=u,
            recent_ratings=recent_ratings,
            recent_reviews=recent_reviews,
            is_me=True,
            user_roles=user_roles,
            primary_role=primary_role,
        )

    @app.get("/user/<username>")
    def user_profile(username: str):
        conn = None
        try:
            conn = get_conn()
            u = dbx.user_by_username(conn, username)
            if not u:
                abort(404)

            recent_ratings = dbx.recent_ratings_for_user(conn, int(u["id"]), limit=30)
            recent_reviews = dbx.recent_reviews_for_user(conn, int(u["id"]), limit=20)

            # NEW: roles
            user_roles = dbx.roles_for_user(conn, int(u["id"]))
            primary_role = user_roles[0] if user_roles else None

        finally:
            if conn is not None:
                conn.close()

        return render_template(
            "profile.html",
            profile_user=u,
            recent_ratings=recent_ratings,
            recent_reviews=recent_reviews,
            is_me=(session.get("uid") == int(u["id"])),
            user_roles=user_roles,
            primary_role=primary_role,
        )

    @app.get("/me/customize")
    @login_required
    def customize_profile():
        conn = None
        try:
            conn = get_conn()
            u = dbx.user_by_id(conn, int(session["uid"]))
            if not u:
                abort(404)
        finally:
            if conn is not None:
                conn.close()

        return render_template("profile_customize.html", u=u)


    @app.post("/me/customize")
    @login_required
    def customize_profile_post():
        avatar_url  = (request.form.get("avatar_url") or "").strip()
        banner_url  = (request.form.get("banner_url") or "").strip()
        theme_color = (request.form.get("theme_color") or "").strip()

        conn = None
        try:
            conn = get_conn()
            dbx.update_user_profile_style(conn, int(session["uid"]), avatar_url, banner_url, theme_color)
        finally:
            if conn is not None:
                conn.close()

        flash("Profile updated.", "ok")
        return redirect(url_for("me_profile"))


    
    @app.get("/admin/roles")
    @admin_required
    def admin_roles():
        conn = None
        try:
            conn = get_conn()
            roles = dbx.list_roles(conn)
        finally:
            if conn is not None:
                conn.close()
        return render_template("admin_roles.html", roles=roles)


    @app.post("/admin/roles")
    @admin_required
    def admin_roles_post():
        name = (request.form.get("name") or "").strip()
        color = (request.form.get("color") or "").strip()

        conn = None
        try:
            conn = get_conn()
            try:
                dbx.create_role(conn, name, color)
            except ValueError as e:
                flash(str(e), "error")
                return redirect(url_for("admin_roles"))
            except sqlite3.IntegrityError:
                flash("Role name already exists.", "error")
                return redirect(url_for("admin_roles"))
        finally:
            if conn is not None:
                conn.close()

        flash("Role created.", "ok")
        return redirect(url_for("admin_roles"))


    @app.post("/admin/roles/<int:role_id>/delete")
    @admin_required
    def admin_delete_role(role_id: int):
        conn = None
        try:
            conn = get_conn()
            dbx.delete_role(conn, role_id)
        finally:
            if conn is not None:
                conn.close()
        flash("Role deleted.", "ok")
        return redirect(url_for("admin_roles"))


    @app.get("/admin/user/<username>/roles")
    @admin_required
    def admin_user_roles(username: str):
        conn = None
        try:
            conn = get_conn()
            u = dbx.user_by_username(conn, username)
            if not u:
                abort(404)

            roles = dbx.list_roles(conn)
            user_roles = dbx.roles_for_user(conn, int(u["id"]))
            user_role_ids = {int(r["id"]) for r in user_roles}

        finally:
            if conn is not None:
                conn.close()

        return render_template(
            "admin_user_roles.html",
            u=u,
            roles=roles,
            user_roles=user_roles,
            user_role_ids=user_role_ids,
        )



    @app.post("/admin/user/<username>/roles/assign")
    @admin_required
    def admin_user_roles_assign(username: str):
        role_id = int(request.form.get("role_id") or "0")
        conn = None
        try:
            conn = get_conn()
            u = dbx.user_by_username(conn, username)
            if not u:
                abort(404)
            dbx.assign_role_to_user(conn, int(u["id"]), role_id, assigned_by_user_id=int(session["uid"]))
        finally:
            if conn is not None:
                conn.close()
        flash("Role assigned.", "ok")
        return redirect(url_for("admin_user_roles", username=username))


    @app.post("/admin/user/<username>/roles/remove")
    @admin_required
    def admin_user_roles_remove(username: str):
        role_id = int(request.form.get("role_id") or "0")
        conn = None
        try:
            conn = get_conn()
            u = dbx.user_by_username(conn, username)
            if not u:
                abort(404)
            dbx.remove_role_from_user(conn, int(u["id"]), role_id)
        finally:
            if conn is not None:
                conn.close()
        flash("Role removed.", "ok")
        return redirect(url_for("admin_user_roles", username=username))



    @app.get("/admin/roles/<int:role_id>/edit")
    @admin_required
    def admin_edit_role(role_id: int):
        conn = None
        try:
            conn = get_conn()
            r = dbx.role_by_id(conn, role_id)
            if not r:
                abort(404)
        finally:
            if conn is not None:
                conn.close()

        return render_template("admin_role_edit.html", r=r)


    @app.post("/admin/roles/<int:role_id>/edit")
    @admin_required
    def admin_edit_role_post(role_id: int):
        name = (request.form.get("name") or "").strip()
        color = (request.form.get("color") or "").strip()

        conn = None
        try:
            conn = get_conn()
            try:
                dbx.update_role(conn, role_id, name, color)
            except ValueError as e:
                flash(str(e), "error")
                return redirect(url_for("admin_edit_role", role_id=role_id))
            except sqlite3.IntegrityError:
                flash("That role name already exists.", "error")
                return redirect(url_for("admin_edit_role", role_id=role_id))
        finally:
            if conn is not None:
                conn.close()

        flash("Role updated.", "ok")
        return redirect(url_for("admin_roles"))


    @app.post("/show/<int:show_id>/review/<int:review_user_id>/vote")
    @login_required
    def vote_review(show_id: int, review_user_id: int):
        vote = int(request.form.get("vote") or "0")
        if vote not in (1, -1):
            abort(400)

        voter_user_id = int(session["uid"])

        # Optional: prevent voting on your own review
        if voter_user_id == int(review_user_id):
            flash("You can't vote on your own review.", "error")
            return redirect(url_for("show_detail", show_id=show_id))

        conn = None
        try:
            conn = get_conn()
            dbx.set_review_vote(conn, show_id, review_user_id, voter_user_id, vote)
        finally:
            if conn is not None:
                conn.close()

        return redirect(url_for("show_detail", show_id=show_id))





    @app.get("/top")
    def top_shows():
        mode = (request.args.get("mode") or "top").strip().lower()
        if mode not in ("top", "bottom"):
            mode = "top"

        conn = None
        try:
            conn = get_conn()
            rows = dbx.list_top_shows_with_top_review(conn, limit=200, mode=mode)
        finally:
            if conn is not None:
                conn.close()

        return render_template("top_shows.html", rows=rows, mode=mode)




    @app.errorhandler(403)
    def forbidden(_e):
        return render_template("base.html", body="Forbidden"), 403

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)