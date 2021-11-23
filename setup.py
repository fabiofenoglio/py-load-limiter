import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
	name = 'py_load_limiter',
	version = '0.1-beta',
	author = 'Fabio Fenoglio',
	author_email = 'development@fabiofenoglio.it',
	long_description=long_description, # Long description read from the the readme file
    long_description_content_type="text/markdown",
	packages = ['py_load_limiter'],
	license = 'MIT',
	description = 'Python variable-sized load limiter',
	url = 'https://github.com/fabiofenoglio/py-load-limiter',
	download_url = 'https://github.com/fabiofenoglio/py-load-limiter/archive/v0.1-beta.tar.gz',
	keywords = ['LIMITER', 'LOAD LIMITER', 'RATE LIMITER'],
	install_requires = [],
	classifiers = [
        # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
		'Development Status :: 4 - Beta',
		'Intended Audience :: Developers',
		'Topic :: Software Development :: Build Tools',
		'License :: OSI Approved :: MIT License',
        "Operating System :: OS Independent",
		'Programming Language :: Python :: 3.6',
		'Programming Language :: Python :: 3.7',
		'Programming Language :: Python :: 3.8',
		'Programming Language :: Python :: 3.9',
		'Programming Language :: Python :: 3.10',
	],
)
