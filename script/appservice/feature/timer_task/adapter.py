from script.appservice.feature.analysis.analysis import Analysis
from script.appservice.feature.report.report import Match


def recommend_match_job():
    rec_match = Match()
    rec_match.to_do()


def analysis_job():
    analysis = Analysis('/opt/logs/Matches_hours_record')
    analysis.start_statistics()
