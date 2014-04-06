#!/usr/bin/env python

import argparse
import enum
import fcntl
import os
from os import path
import re
import shutil
import sqlite3
import subprocess
import sys
import traceback

# Define verbosity levels.
Level = enum.Enum(
    "Fatal",
    "Error",
    "Info",
    "Verbose",
)

# Define meta files.
LockFile = ".lock"
DbFile = "meta.db"

# Status of items.
Status = enum.Enum(
    "Archived",
    "Restored",
    "Deleted",
    "Corrupted",
)

def log(level, *args):
  if level <= verbosity:
    prefix = str(level) + ": "
    print >> sys.stderr, prefix + "".join(map(str, args))
  if level == Level.Fatal:
    traceback.print_stack()
    sys.exit(1)

def parseArgs(argv):
  parser = argparse.ArgumentParser(
      description="Recycle Bin for Linux",
      fromfile_prefix_chars="@")
  parser.add_argument(
      "-r", "--root",
      required=True,
      help="The path to the repository, the directory that holds the archive data")
  parser.add_argument(
      "--v", "--verbose",
      dest="verbose",
      choices=list(Level),
      nargs="?",
      type=lambda v: getattr(Level, v),
      default="Error",
      const="Info",
      help="Verbose level of debugging output (default Error)")

  subparsers = parser.add_subparsers(help="commands")

  parserArchive = subparsers.add_parser("a", help="Archive the file or directory")
  parserArchive.add_argument("source", help="The file or directory to archive")
  parserArchive.set_defaults(handler=archive)

  parserRestore = subparsers.add_parser("r", help="Restore the item")
  parserRestore.add_argument("item", help="The item to restore")
  parserRestore.add_argument(
      "-d", "--directory",
      help="The directory to restore to instead of the original directory")
  parserRestore.add_argument(
      "-v", "--version",
      type=int,
      help="If multiple items have the same name, use this to distinguish them")
  parserRestore.set_defaults(handler=restore)

  parserList = subparsers.add_parser("l", help="Print items")
  parserList.add_argument(
      "item",
      nargs="?",
      type=re.compile,
      default=".*",
      help="The regular expression pattern of items to print")
  parserList.set_defaults(handler=listItems)

  parserConfig = subparsers.add_parser("c", help="Configure the repository")
  parserConfig.add_argument(
      "-s", "--size",
      nargs="?",
      default=argparse.SUPPRESS,
      const=None,
      help="Set or get the total size limit of the archive data")
  parserConfig.set_defaults(handler=config)

  archiverrc = path.expanduser("~/.archiverrc")
  if path.isfile(archiverrc):
    argv.insert(0, "@" + archiverrc)
  return parser.parse_args(argv)

def exclusiveLock(file):
  log(Level.Info, "Waiting for exclusive lock on file `", file, "`")
  fcntl.lockf(os.open(file, os.O_RDWR | os.O_CREAT), fcntl.LOCK_EX)

def initializeDatabase(conn):
  cursor = conn.cursor()
  cursor.executescript(
      """CREATE TABLE IF NOT EXISTS items (
             name TEXT NOT NULL CHECK(name <> ''),
             version INTEGER CHECK(version >= 0),
             timestamp TEXT,
             status TEXT,
             source TEXT NOT NULL CHECK(source <> ''),
             archive TEXT UNIQUE NOT NULL CHECK(archive <> ''),
             PRIMARY KEY (name, version));
         CREATE TABLE IF NOT EXISTS versions (
             name TEXT NOT NULL CHECK(name <> ''),
             version INTEGER CHECK(version >= 0),
             PRIMARY KEY (name));
         CREATE TABLE IF NOT EXISTS config (
             key TEXT NOT NULL CHECK(key <> ''),
             value TEXT NOT NULL CHECK(value <> ''),
             PRIMARY KEY (key));

         CREATE TRIGGER IF NOT EXISTS config_records_not_deletable
         BEFORE DELETE ON config
         BEGIN
             SELECT RAISE(ABORT, 'Records in config table cannot be deleted');
         END;

         INSERT OR IGNORE INTO config VALUES
             ("size", "10737418240");
      """)
  conn.commit()

def deleteFileOrDirectory(root, file):
  fullPath = path.join(root, file)
  def errorHandler(_, path, e):
    log(Level.Error, "Failed to cleanup file `", path, "`: ", e)
  if path.isfile(fullPath):
    log(Level.Info, "Deleting file `", file, "`")
    try:
      os.remove(fullPath)
    except Exception as e:
      errorHandler(None, fullPath, e)
  elif path.isdir(fullPath):
    log(Level.Info, "Deleting directory `", file, "`")
    shutil.rmtree(fullPath, onerror=errorHandler)

def cleanup(conn, root, **kwargs):
  cursor = conn.cursor()
  cursor.execute("SELECT archive FROM items WHERE status = 'Archived' ORDER BY timestamp ASC")
  archives = [row["archive"] for row in cursor.fetchall()]
  excludeFromDeletion = set(archives)
  limitSize = getConfig(conn, "size")
  archiveSizes = dict()
  totalSize = 0
  for archive in archives:
    archiveSize = os.stat(path.join(root, archive)).st_size
    log(Level.Verbose, "Size of archive `", archive, "`: ", archiveSize)
    archiveSizes[archive] = archiveSize
    totalSize += archiveSize
  log(Level.Info, "Total size of existing archive: ", totalSize)
  for archive in archives:
    if totalSize > limitSize:
      log(Level.Info, "Decide to delete `", archive, "` because of total size limit")
      cursor.execute("UPDATE items SET status = 'Deleted' WHERE name = :name", {"name": archive})
      totalSize -= archiveSizes[archive]
      excludeFromDeletion.discard(archive)
  conn.commit()
  excludeFromDeletion.update([DbFile, LockFile])
  for file in os.listdir(root):
    if file not in excludeFromDeletion:
      deleteFileOrDirectory(root, file)

def archive(conn, root, source, **kwargs):
  source = path.abspath(source)
  dir = path.dirname(source)
  base = path.basename(source)
  cursor = conn.cursor()
  cursor.execute("INSERT OR IGNORE INTO versions VALUES (:name, 0)", {"name": base})
  cursor.execute("UPDATE versions SET version = version + 1 WHERE name = :name", {"name": base})
  cursor.execute("SELECT version FROM versions WHERE name = :name", {"name": base})
  version = cursor.fetchone()["version"]
  conn.commit()
  archive = "{base}-{version}.tar.gz".format(**locals())
  fullPath = path.join(root, archive)
  commands = ["tar", "--checkpoint=.10000", "-C", dir, "-czf", fullPath, base]
  log(Level.Info, "Running command ", " ".join(commands))
  subprocess.check_call(commands)
  print
  cursor.execute(
      """INSERT INTO items VALUES (
             :name,
             :version,
             CURRENT_TIMESTAMP,
             'Archived',
             :source,
             :archive);
      """,
      {
        "name": base,
        "version": version,
        "source": source,
        "archive": archive,
      })
  conn.commit()

def getConfig(conn, key):
  Converters = {
      "size": int,
  }
  if key not in Converters:
    log(Level.Fatal, "Invalid key `", key, "`")
  cursor = conn.cursor()
  cursor.execute("SELECT value FROM config WHERE key = :key", {"key": key})
  row = cursor.fetchone()
  if row is None:
    log(Level.Fatal, "Key `", key, "` not found in config table")
  return Converters[key](row["value"])

def convertSizeStringToInt(s):
  SuffixToMultiplier = {
      "k": 10**3,
      "m": 10**6,
      "g": 10**9,
      "t": 10**12,
      "K" : 1 << 10,
      "M" : 1 << 20,
      "G" : 1 << 30,
      "T" : 1 << 40,
  }
  size = 1
  if s[-1] in SuffixToMultiplier:
    size = SuffixToMultiplier[s[-1]]
    s = s[:-1]
  size = int(size * float(s))
  if size <= 0:
    log(Level.Fatal, "config.size `", size, "` should be positive")
  return size

def config(conn, root, **kwargs):
  Converters = {
      "size": convertSizeStringToInt,
  }
  cursor = conn.cursor()
  keys = set(kwargs.iterkeys()).intersection(set(Converters.iterkeys()))
  if keys:
    for key in keys:
      value = kwargs[key]
      if value is None:
        print "config.%s=%r" % (key, getConfig(conn, key))
      else:
        converter = Converters[key]
        cursor.execute("UPDATE config SET value = :value WHERE key = :key", {"key": key, "value": converter(value)})
  else:
    for key in Converters:
      print "config.%s=%r" % (key, getConfig(conn, key))
  conn.commit()

def restore(conn, root, item, version, directory, **kwargs):
  cursor = conn.cursor()
  sql = "SELECT * FROM items WHERE status = 'Archived' AND name = :name"
  if version is not None:
    sql += " AND version = :version"
  cursor.execute(sql, {"name": item, "version": version})
  rows = list(cursor.fetchall())
  if not rows:
    message = ["There is no archived item named `", item, "`"]
    if version is not None:
      message += [" and version ", version]
    log(Level.Fatal, *message)
  elif len(rows) > 1:
    log(Level.Fatal, "The item name `", item, "` is ambiguous")
  row = rows[0]
  if directory is None:
    directory = path.dirname(row["source"])
  fullPath = path.join(root, row["archive"])
  commands = ["tar", "--checkpoint=.10000", "-C", directory, "-xzf", fullPath]
  log(Level.Info, "Running command ", " ".join(commands))
  returncode = subprocess.call(commands)
  print
  status = "Restored" if returncode == 0 else "Corrupted"
  cursor.execute(
      "UPDATE items SET status = :status WHERE name = :name AND version = :version",
      {"name": item, "version": row["version"], "status": status})
  conn.commit()

def listItems(conn, root, item, **kwargs):
  cursor = conn.cursor()
  cursor.execute("SELECT * FROM items ORDER BY timestamp ASC")
  def printRow(row, index):
    print "#%03d   " % index + " ".join(["%s" % row[key] for key in row.keys()])
  index = 1
  for row in cursor.fetchall():
    if item.match(row["name"]):
      printRow(row, index)
      index += 1

def main():
  args = parseArgs(sys.argv[1:])
  global verbosity
  verbosity = args.verbose
  # Acquire exclusive lock.
  exclusiveLock(path.join(args.root, LockFile))
  dbfile = path.join(args.root, DbFile)
  with sqlite3.connect(dbfile, isolation_level="EXCLUSIVE", detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as conn:
    initializeDatabase(conn)
    conn.row_factory = sqlite3.Row
    args.handler(conn, **vars(args))
    cleanup(conn, **vars(args))

if __name__ == "__main__":
  sys.exit(main())
