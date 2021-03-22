#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tournament_info import TournamentInfo
from tournament_admin import TournamentAdmin
from tournament_submit import TournamentSubmit


class Tournament(TournamentAdmin, TournamentInfo, TournamentSubmit):
    """Tournament Commands"""
