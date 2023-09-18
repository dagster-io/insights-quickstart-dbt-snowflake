from setuptools import find_packages, setup

setup(
    name="purina_usage",
    packages=find_packages(exclude=["purina_usage_tests*"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "snowflake-connector-python[pandas]",
        "snowflake.sqlalchemy",
        "dagster-slack",
        "dagster-snowflake",
        "dagster-snowflake-pandas",
        "boto3",
        "dagster-dbt",
        "dagster-graphql",
        "pandas",
        "numpy",
        "scipy",
        "dbt-core",
        "dbt-snowflake",
        # packaging v22 has build compatibility issues with dbt as of 2022-12-07
        # "packaging<22.0",
    ],
    extras_require={
        "dev": [
            "dagit",
            "pytest",
            "dbt-postgres",
        ]
    },
)
