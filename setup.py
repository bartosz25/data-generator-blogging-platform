from setuptools import setup, find_packages
setup(
    name="Data generator",
    version="1.0.0",
    description="Generates the dataset for testing data applications",
    author="Bartosz Konieczny",
    packages=find_packages(),
    install_requires=['confluent-kafka==2.3.10', 'Faker==20.1.0',
                      'psycopg2-binary==2.9.9'],
    project_urls={
        'Contact form': 'https://www.waitingforcode.com/static/contact'
    }
)
