"""Authorization of the application: only Bitrix24 employees open it.

Bitrix24 opens the application with AUTH_ID, a key it gave to one employee.
The server asks the portal whose key it is (bitrix.py) and then issues its own
signed key (tokens.py). The middleware (middleware.py) closes every request
without a valid key and takes the user of a request from the key only.
"""
