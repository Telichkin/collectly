#!/usr/bin/env bash
export DATABASE_URI="sqlite:///$(pwd)/database.db"
gunicorn collectly.api
