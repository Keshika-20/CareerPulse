from src.analytics.trend_analytics import average_salary_by_company, build_summary


def test_average_salary_ignores_unknown_values_without_imputing_zero():
    records = [
        {"company": "Acme", "salary": 100000},
        {"company": "Acme", "salary": None},
        {"company": "Unknown Salary Co", "salary": None},
    ]

    assert average_salary_by_company(records) == [
        {"company": "Acme", "average_salary": 100000.0},
        {"company": "Unknown Salary Co", "average_salary": None},
    ]


def test_empty_scalar_salary_summary_does_not_infer_from_salary_ranges():
    records = [
        {
            "company": "Acme",
            "salary": None,
            "salary_min": 80000,
            "salary_max": 120000,
            "event": "NEW",
        }
    ]

    assert build_summary(records)["average_salary_by_company"] == [
        {"company": "Acme", "average_salary": None}
    ]
