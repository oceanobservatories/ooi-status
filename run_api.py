#!/usr/bin/env python
from ooi_status.api import app


def main():
    app.run(host='0.0.0.0', port=9000, debug=True)


if __name__ == '__main__':
    main()
