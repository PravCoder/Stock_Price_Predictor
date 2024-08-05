PyJKS enables Python projects to load and manipulate Java KeyStore
(JKS) data without a JVM dependency. PyJKS supports JKS, JCEKS, BKS
and UBER (BouncyCastle) keystores. Simply::

  pip install pyjks

Or::

  easy_install pyjks

Then::

  import jks

  keystore = jks.KeyStore.load('keystore.jks', 'passphrase')

  print(keystore.private_keys)
  print(keystore.certs)
  print(keystore.secret_keys)

And that's barely scratching the surface. Check out `the usage examples on
GitHub <https://github.com/kurtbrose/pyjks#usage-examples>`_ for
more!



