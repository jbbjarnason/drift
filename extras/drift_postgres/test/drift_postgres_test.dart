@TestOn('vm')
library;

import 'dart:async';

import 'package:drift_postgres/drift_postgres.dart';
import 'package:drift_testcases/tests.dart';
import 'package:postgres/postgres.dart' as pg;
import 'package:test/test.dart';

final _endpoint = pg.Endpoint(
  host: 'localhost',
  database: 'postgres',
  username: 'postgres',
  password: 'postgres',
);

class PgExecutor extends TestExecutor {
  /// When set, databases are opened on a [pg.Pool] instead of a single
  /// connection.
  final bool usePool;

  final List<pg.Pool> _openedPools = [];

  PgExecutor({this.usePool = false});

  @override
  bool get supportsReturning => true;

  // Transactions on a pool are run with `Pool.runTx`, which doesn't support
  // savepoints.
  @override
  bool get supportsNestedTransactions => !usePool;

  @override
  DatabaseConnection createConnection() {
    if (usePool) {
      final pool = pg.Pool.withEndpoints(
        [_endpoint],
        settings: pg.PoolSettings(
          maxConnectionCount: 4,
          sslMode: pg.SslMode.disable,
        ),
      );
      _openedPools.add(pool);
      return DatabaseConnection(PgDatabase.opened(pool));
    }

    return DatabaseConnection(PgDatabase(
      endpoint: _endpoint,
      settings: pg.ConnectionSettings(sslMode: pg.SslMode.disable),
    ));
  }

  @override
  Future clearDatabaseAndClose(GeneratedDatabase db) async {
    await db.customStatement('DROP SCHEMA public CASCADE;');
    await db.customStatement('CREATE SCHEMA public;');
    await db.customStatement('GRANT ALL ON SCHEMA public TO postgres;');
    await db.customStatement('GRANT ALL ON SCHEMA public TO public;');
    await db.close();
  }

  @override
  Future<void> deleteData() async {}

  Future<void> closePools() async {
    for (final pool in _openedPools) {
      await pool.close();
    }
    _openedPools.clear();
  }
}

void main() {
  runAllTests(PgExecutor());

  group('pool', () {
    final executor = PgExecutor(usePool: true);
    tearDownAll(executor.closePools);

    runAllTests(executor);
  });

  // Regression for https://github.com/simolus3/drift/issues/2981
  test('bind null to nullable column', () async {
    final executor = PgExecutor();
    final db = Database(executor.createConnection());

    await db.customStatement('''
CREATE TABLE mytable (
  id INTEGER PRIMARY KEY NOT NULL,
  value INTEGER
);
''');

    // Provide null to a nullable column
    await db.customInsert(
      r'INSERT INTO mytable (id, value) VALUES (1, $1);',
      variables: [Variable(null)],
    );

    await executor.clearDatabaseAndClose(db);
  });

  group('transactions on a pool', () {
    late PgExecutor executor;
    late Database db;
    late pg.Pool pool;

    setUp(() async {
      executor = PgExecutor(usePool: true);
      db = Database(executor.createConnection());
      pool = executor._openedPools.single;

      await db.customStatement(
          'CREATE TABLE pool_test (id INTEGER PRIMARY KEY NOT NULL);');
    });

    tearDown(() async {
      await executor.clearDatabaseAndClose(db);
      await executor.closePools();
    });

    Future<int> backendPid() async {
      final row =
          await db.customSelect('SELECT pg_backend_pid() AS pid;').getSingle();
      return row.read<int>('pid');
    }

    Future<int> rowCount() async {
      final row = await db
          .customSelect('SELECT COUNT(*) AS c FROM pool_test;')
          .getSingle();
      return row.read<int>('c');
    }

    Future<void> insert(int id) async {
      await db.customStatement('INSERT INTO pool_test (id) VALUES ($id);');
    }

    test('runs all statements on one connection', () async {
      await db.transaction(() async {
        final pid = await backendPid();

        // Keep another pool connection busy while the transaction is running,
        // the transaction must not be affected by that.
        await pool.withConnection((_) async {
          await insert(1);
          expect(await backendPid(), pid);
        });

        await insert(2);
        expect(await backendPid(), pid);
      });

      expect(await rowCount(), 2);
    });

    test('rolls back when the transaction throws', () async {
      final exception = Exception('rollback');

      await expectLater(
        db.transaction(() async {
          await pool.withConnection((_) async {
            await insert(1);
          });
          await insert(2);
          expect(await rowCount(), 2);

          throw exception;
        }),
        throwsA(exception),
      );

      expect(await rowCount(), 0);

      // The database should still be usable afterwards.
      await db.transaction(() => insert(3));
      expect(await rowCount(), 1);
    });

    test('concurrent transactions are isolated', () async {
      final firstInserted = Completer<void>();
      final secondDone = Completer<void>();

      final first = db.transaction(() async {
        await insert(1);
        firstInserted.complete();
        await secondDone.future;

        // The second transaction runs on another connection, so only our own
        // write is visible here.
        expect(await rowCount(), 1);
      });

      final second = db.transaction(() async {
        await firstInserted.future;
        await insert(2);
        expect(await rowCount(), 1);
        secondDone.complete();
      });

      await Future.wait([first, second]).timeout(const Duration(seconds: 10));
      expect(await rowCount(), 2);
    });
  });
}
