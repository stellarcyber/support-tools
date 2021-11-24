#!/usr/bin/env python3
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import subprocess
import json
import argparse
import time
import tempfile
import os
import re


class BlobOperatorException(Exception):
    pass


class BlobOperator:
    BLOB_TIER_KEY = 'StellarBlobTier'
    BLOB_TIER_ARCHIVE = 'archive'
    BLOB_TIER_HOT = 'hot'

    VENDOR_AZURE = "azure"
    VENDOR_AWS = "aws"

    EXCLUDED_INDICES_FILE = "stellar_data_backup/stellar_excluded_indices"

    def __init__(self, trace_enabled=False):
        self.trace_enabled = trace_enabled

    def trace(self, fmt, *args):
        if self.trace_enabled:
            self.log(fmt, *args)

    @staticmethod
    def log(fmt, *args):
        if args:
            print(fmt % args)
        else:
            print(fmt)

    def avoid_throttling(self, duration):
        self.log("sleep %d seconds to avoid throttling...", duration)
        time.sleep(duration)

    @classmethod
    def get_operator(cls, vendor, args: argparse.Namespace):
        if vendor == cls.VENDOR_AWS:
            return S3Operator(args)
        elif vendor == cls.VENDOR_AZURE:
            return AzureBlobOperator(args)
        raise NotImplemented

    def get_index_metadata(self, get_index_blobs_fn, download_file_fn):
        blobs = get_index_blobs_fn()
        if len(blobs) != 1:
            self.log('find incorrect number of index files (expected one): %d', len(blobs))
            return
        metadata = {'indices': {}}
        with tempfile.TemporaryDirectory() as d:
            filename = f'{d}/index.json'
            download_file_fn(blobs[0], filename)
            if os.path.isfile(filename):
                with open(filename) as fh:
                    metadata = json.load(fh)
                os.remove(filename)
        return metadata

    def get_excluded_indices(self, excluded_indices_file, download_file_fn):
        indices = []
        with tempfile.TemporaryDirectory() as d:
            filename = f'{d}/excluded_indices.json'
            try:
                download_file_fn(excluded_indices_file, filename)
                if os.path.isfile(filename):
                    with open(filename) as fh:
                        indices = json.load(fh)
                    os.remove(filename)
            except Exception as e:
                self.log("failed to read %s: %s", excluded_indices_file, e)
        return indices

    def do_get_prefix(self, index_names, get_index_blobs_fn, download_file_fn):
        blobs = get_index_blobs_fn()
        if len(blobs) != 1:
            self.log('find incorrect number of index files (expected one): %d', len(blobs))
            return
        metadata = self.get_index_metadata(get_index_blobs_fn, download_file_fn)
        index_ids = []
        missing_names = []
        for index_name in index_names.strip().split(','):
            index_id = metadata.get('indices', {}).get(index_name, {}).get('id')
            index_ids.append(index_id)
            if not index_id:
                missing_names.append(index_name)
        if not index_ids:
            self.log(f'failed to find index id of indices {missing_names}')
        return index_ids

    def should_skip_index(self, index_id, dst_tier, excluded_indices):
        dst_tier_key = dst_tier.lower()
        return dst_tier_key == self.BLOB_TIER_ARCHIVE and index_id in excluded_indices

    def update_excluded_index(self, indices_diff, dst_tier, excluded_indices_file, download_fn, upload_fn):
        dst_tier_key = dst_tier.lower()
        if dst_tier_key == self.BLOB_TIER_HOT:
            self.add_excluded_indices(indices_diff, excluded_indices_file, download_fn, upload_fn)
        elif dst_tier_key == self.BLOB_TIER_ARCHIVE:
            self.remove_excluded_indices(indices_diff, excluded_indices_file, download_fn, upload_fn)

    @staticmethod
    def _update_excluded_indices(
            index_ids, excluded_indices_file, download_file_fn, upload_file_fn, update_indices_fn):
        with tempfile.TemporaryDirectory() as d:
            filename = f'{d}/excluded_indices.json'
            download_file_fn(excluded_indices_file, filename, ignore_error=True)
            if os.path.isfile(filename):
                with open(filename, 'r') as fh:
                    indices = json.load(fh)
                    existing_indices = set(indices)
                    update_indices_fn(index_ids, indices, existing_indices)
            else:
                indices = []

            with open(filename, 'w') as fh:
                fh.write(json.dumps(indices))

            upload_file_fn(excluded_indices_file, filename)
            if os.path.isfile(filename):
                os.remove(filename)

    def add_excluded_indices(self, index_ids, excluded_indices_file, download_file_fn, upload_file_fn):
        def add_indices(ids, indices, existing_indices):
            for index_id in ids:
                if index_id not in existing_indices:
                    existing_indices.add(index_id)
                    indices.append(index_id)

        self._update_excluded_indices(
            index_ids, excluded_indices_file, download_file_fn, upload_file_fn, add_indices)

    def remove_excluded_indices(self, index_ids, excluded_indices_file, download_file_fn, upload_file_fn):
        def remove_indices(ids, indices, existing_indices):
            for index_id in ids:
                if index_id in existing_indices:
                    existing_indices.discard(index_id)
                    indices.remove(index_id)

        self._update_excluded_indices(
            index_ids, excluded_indices_file, download_file_fn, upload_file_fn, remove_indices)

    def restore(self, *args):
        raise NotImplemented

    def archive(self, *args):
        raise NotImplemented

    def sync(self, *args):
        raise NotImplemented

    def tag(self, *args):
        raise NotImplemented

    def get_prefix(self, *args):
        raise NotImplemented


class S3Operator(BlobOperator):
    THROTTLING_SECONDS = 60

    STORAGE_CLASS_LOOKUP = {
        BlobOperator.BLOB_TIER_ARCHIVE: "DEEP_ARCHIVE",
        BlobOperator.BLOB_TIER_HOT: "STANDARD",
    }

    COMMON_INDEX_PREFIX = 'stellar_data_backup//indices'

    INDEX_ID_RE = re.compile(r's3://[^/]+/{}/([^/]+)/'.format(COMMON_INDEX_PREFIX))
    BLOB_INDEX_ID_RE = re.compile(r'^{}/([^/]+)/'.format(COMMON_INDEX_PREFIX))

    def __init__(self, args: argparse.Namespace):
        super(S3Operator, self).__init__(args.trace_enabled)
        self.bucket = args.bucket

    def get_storage_class(self, tier):
        return self.STORAGE_CLASS_LOOKUP.get(tier.lower(), 'UNKNOWN')

    @staticmethod
    def get_blobs_query(args):
        return f'--prefix {args.included_prefix} --page-size {args.page_size}'

    def get_blobs(self, args, src_tier, force):
        marker = ''
        found_next_marker = True
        query = self.get_blobs_query(args)

        expected = self.get_storage_class(src_tier)
        while found_next_marker:
            found_next_marker = False
            proc = subprocess.Popen(
                f'aws s3api list-objects-v2 --bucket {self.bucket} {marker} {query}',
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
            )
            out, err = proc.communicate()
            if proc.returncode != 0:
                raise BlobOperatorException(f"failed to list blobs: {err.decode()}")
            resp = json.loads(out)
            for data in resp['Contents']:
                if 'Key' in data:
                    name = data['Key']
                    if not force and data['StorageClass'] != expected:
                        self.trace("skip processing blob %s because mismatched storage class (actual %s, expected %s)",
                                   name, data['StorageClass'], expected)
                        continue
                    yield name
            if resp.get('NextToken'):
                next_marker = resp.get('NextToken')
                found_next_marker = True
                marker = f'--starting-token \'{next_marker}\''

    def _set_tag(self, blob, dst_tier, errors):
        proc = subprocess.Popen(
            f'aws s3api put-object-tagging --bucket {self.bucket} --key {blob} '
            f'--tagging \'{json.dumps({"TagSet": [{"Key": self.BLOB_TIER_KEY, "Value": dst_tier}]})}\'',
            stderr=subprocess.PIPE, shell=True,
        )
        _, err = proc.communicate()
        if proc.returncode != 0:
            errors.append(f"{blob}: {err.decode()}")
        else:
            self.log("set tags %s", blob)
        return True

    def set_tag(self, blobs, dst_tier, excluded_indices_enabled=False):
        errors = []
        processed = 0
        indices_diff = set()
        excluded_indices = set(self.get_excluded_indices(self.EXCLUDED_INDICES_FILE, self.download))

        for blob in blobs:
            m = self.BLOB_INDEX_ID_RE.match(blob)
            if not m:
                continue

            index_id = m.group(1)
            indices_diff.add(index_id)
            if excluded_indices_enabled and self.should_skip_index(index_id, dst_tier, excluded_indices):
                continue

            while not self._set_tag(blob, dst_tier, errors):
                self.avoid_throttling(self.THROTTLING_SECONDS)
            processed += 1

        if errors:
            self.log("failed to set tags:\n%s", "\n".join(errors))
        else:
            if excluded_indices_enabled and indices_diff:
                self.update_excluded_index(
                    indices_diff, dst_tier, self.EXCLUDED_INDICES_FILE, self.download, self.upload)
            self.log("set tags for %d blobs", processed)

    def _restore(self, blob, days, errors):
        proc = subprocess.Popen(
            f'aws s3api restore-object --bucket {self.bucket} '
            f'--key {blob} --restore-request \'{{"Days":{days},"GlacierJobParameters":{{"Tier":"Standard"}}}}\'',
            stderr=subprocess.PIPE, shell=True,
        )
        _, err = proc.communicate()
        if proc.returncode != 0:
            errors.append(f"{blob}: {err.decode()}")
        else:
            self.log("restore %s", blob)
        return True

    def _sync(self, included_prefix, excluded_indices_enabled, errors):
        proc = subprocess.Popen(
            f'aws s3 cp s3://{self.bucket}/{included_prefix} s3://{self.bucket}/{included_prefix} '
            f'--storage-class STANDARD --recursive --force-glacier-transfer',
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
        )
        out, err = proc.communicate()
        if proc.returncode != 0:
            errors.append(f"{included_prefix}: {err.decode()}")
        else:
            excluded_indices = set()
            # output example: copy: s3://mybucket/test.txt to s3://mybucket2/test.txt
            for line in out.decode().strip().split("\n"):
                tokens = line.strip().split()
                if len(tokens) != 4:
                    self.log("WARNING: unexpected output format: %s", line)
                    continue
                m = self.INDEX_ID_RE.match(tokens[1])
                if m:
                    excluded_indices.add(m.group(1))

            if excluded_indices_enabled:
                self.add_excluded_indices(
                    excluded_indices, self.EXCLUDED_INDICES_FILE, self.download, self.upload)
            else:
                self.log("update excluded indices is disabled")
            self.log("set storage class with prefix %s", included_prefix)
        return True

    def restore(self, args: argparse.Namespace):
        blobs = self.get_blobs(args, self.BLOB_TIER_ARCHIVE, False)
        errors = []
        processed = 0
        for blob in blobs:
            while not self._restore(blob, args.restore_days, errors):
                self.avoid_throttling(self.THROTTLING_SECONDS)
            processed += 1
        if errors:
            self.log("failed to restore:\n%s", "\n".join(errors))
        else:
            self.log("restore for %d blobs", processed)

    def sync(self, args: argparse.Namespace):
        errors = []
        self._sync(args.included_prefix, args.excluded_indices_enabled, errors)
        if errors:
            self.log("failed to set storage class:\n%s", "\n".join(errors))
            return

        blobs = self.get_blobs(args, self.BLOB_TIER_ARCHIVE, False)
        dst_tier = self.BLOB_TIER_HOT.capitalize()
        self.set_tag(blobs, dst_tier)

    def tag(self, args: argparse.Namespace):
        dst_tier = args.dst_tier.capitalize()
        blobs = self.get_blobs(args, args.src_tier, args.force)
        self.set_tag(blobs, dst_tier, args.excluded_indices_enabled)

    def get_index_blobs(self):
        proc = subprocess.Popen(
            f'aws s3api list-objects-v2 --bucket {self.bucket} '
            f'--prefix \'stellar_data_backup/index-\' '
            f'--query \'Contents[].Key\'',
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
        )
        out, err = proc.communicate()
        if proc.returncode != 0:
            raise BlobOperatorException(f'failed to list index blob: {err.decode()}')
        return json.loads(out)

    def download(self, name, filename, ignore_error=False):
        proc = subprocess.Popen(
            f'aws s3 cp s3://{self.bucket}/{name} {filename}',
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
        )
        _, err = proc.communicate()
        if not ignore_error and proc.returncode != 0:
            raise BlobOperatorException(f'failed to download {name}: {err.decode()}')
        elif proc.returncode == 0:
            self.log(f'download file {name} to {filename}')

    def upload(self, name, filename):
        proc = subprocess.Popen(
            f'aws s3 cp {filename} s3://{self.bucket}/{name}',
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
        )
        _, err = proc.communicate()
        if proc.returncode != 0:
            raise BlobOperatorException(f'failed to upload {name}: {err.decode()}')
        self.log(f'upload file {filename} to {name}')

    def get_prefix(self, args: argparse.Namespace):
        index_ids = self.do_get_prefix(args.names, self.get_index_blobs, self.download)
        if index_ids:
            print(f"get prefix of indices: {self.COMMON_INDEX_PREFIX}/\n")
            print(f"get id of indices: {index_ids}\n")
            print(f'get id of indices (for bash): {" ".join(index_ids)}')


class AzureBlobOperator(BlobOperator):
    THROTTLING_SECONDS = 120

    COMMON_INDEX_PREFIX = 'stellar_data_backup/indices'

    BLOB_INDEX_ID_RE = re.compile(r'^{}/([^/]+)/'.format(COMMON_INDEX_PREFIX))

    def __init__(self, args: argparse.Namespace):
        super(AzureBlobOperator, self).__init__(args.trace_enabled)
        self.account_name = args.account_name
        self.container_name = args.container_name

    @staticmethod
    def get_excluded_blobs_query(args, src_tier, force):
        # used for converting non-index files back to Hot data.
        tier_check = ''
        if not force:
            tier_check = f'&& properties.blobTier == `{src_tier}`'
        return f'--query \'[?name && !starts_with(name, `{args.excluded_prefix}`) {tier_check}]' \
               f'--num-results {args.num_results}'

    @staticmethod
    def get_included_blobs_query(args, src_tier, force):
        query = f'--prefix \'{args.included_prefix}\' --num-results {args.num_results}'
        if not force:
            query = f'{query} --query \'[?properties.blobTier == `{src_tier}`]\''
        return query

    def get_blobs_query(self, args, src_tier, force):
        if args.excluded_prefix:
            return self.get_excluded_blobs_query(args, src_tier, force)
        else:
            return self.get_included_blobs_query(args, src_tier, force)

    def get_blobs(self, args, src_tier, force):
        marker = ''
        found_next_marker = True
        query = self.get_blobs_query(args, src_tier, force)

        while found_next_marker:
            proc = subprocess.Popen(
                f"az storage blob list --show-next-marker {marker}"
                f"--container-name {self.container_name} --account-name {self.account_name} {query} ",
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
            )
            out, err = proc.communicate()
            if proc.returncode != 0:
                err_str = err.decode()
                if self.is_throttled(err_str):
                    self.avoid_throttling(self.THROTTLING_SECONDS)
                    continue
                else:
                    raise BlobOperatorException(f"failed to list blobs: {err.decode()}")

            found_next_marker = False
            resp = json.loads(out)
            for data in resp:
                if 'name' in data:
                    yield data['name']
                elif data.get('nextMarker'):
                    next_marker = data.get('nextMarker')
                    found_next_marker = True
                    marker = f'--marker \'{next_marker}\''

    @staticmethod
    def is_throttled(err):
        return 'TooManyRequests' in err

    def _set_tier(self, blob, dst_tier, errors):
        proc = subprocess.Popen(
            f"az storage blob set-tier --account-name {self.account_name} "
            f"--container-name {self.container_name} "
            f"--name {blob} --tier {dst_tier}",
            stderr=subprocess.PIPE, shell=True,
        )
        _, err = proc.communicate()
        if proc.returncode != 0:
            err_str = err.decode()
            if self.is_throttled(err_str):
                return False
            else:
                errors.append(f"{blob}: {err_str}")
        else:
            self.log("set tier %s", blob)
        return True

    def set_tier(self, blobs, dst_tier):
        errors = []
        processed = 0
        for blob in blobs:
            while not self._set_tier(blob, dst_tier, errors):
                self.avoid_throttling(self.THROTTLING_SECONDS)
            processed += 1
        if errors:
            self.log("failed to set tier:\n%s", "\n".join(errors))
        else:
            self.log("set tier for %d blobs", processed)

    def _set_tag(self, blob, dst_tier, errors):
        proc = subprocess.Popen(
            f"az storage blob tag set --account-name {self.account_name} "
            f"--container-name {self.container_name} "
            f"--name {blob} --tags {f'{self.BLOB_TIER_KEY}={dst_tier}'}",
            stderr=subprocess.PIPE, shell=True,
        )
        _, err = proc.communicate()
        if proc.returncode != 0:
            err_str = err.decode()
            if self.is_throttled(err_str):
                return False
            else:
                errors.append(f"{blob}: {err_str}")
        else:
            self.log("set tags %s", blob)
        return True

    def set_tag(self, blobs, dst_tier, excluded_indices_enabled=False):
        errors = []
        processed = 0
        indices_diff = set()
        excluded_indices = set(self.get_excluded_indices(self.EXCLUDED_INDICES_FILE, self.download))

        for blob in blobs:
            m = self.BLOB_INDEX_ID_RE.match(blob)
            if not m:
                continue

            index_id = m.group(1)
            indices_diff.add(index_id)
            if excluded_indices_enabled and self.should_skip_index(index_id, dst_tier, excluded_indices):
                continue

            while not self._set_tag(blob, dst_tier, errors):
                self.avoid_throttling(self.THROTTLING_SECONDS)
            processed += 1

        if errors:
            self.log("failed to set tags:\n%s", "\n".join(errors))
        else:
            if excluded_indices_enabled and indices_diff:
                self.update_excluded_index(
                    indices_diff, dst_tier, self.EXCLUDED_INDICES_FILE, self.download, self.upload)
            self.log("set tags for %d blobs", processed)

    def restore(self, args: argparse.Namespace):
        src_tier, dst_tier = self.BLOB_TIER_ARCHIVE.capitalize(), self.BLOB_TIER_HOT.capitalize()

        blobs = self.get_blobs(args, src_tier, False)
        self.set_tier(blobs, dst_tier)

        blobs = self.get_blobs(args, src_tier, False)
        self.set_tag(blobs, dst_tier)

    def archive(self, args: argparse.Namespace):
        src_tier, dst_tier = self.BLOB_TIER_HOT.capitalize(), self.BLOB_TIER_ARCHIVE.capitalize()

        blobs = self.get_blobs(args, src_tier, False)
        self.set_tier(blobs, dst_tier)

        blobs = self.get_blobs(args, src_tier, False)
        self.set_tag(blobs, dst_tier)

    def tag(self, args: argparse.Namespace):
        src_tier, dst_tier = args.src_tier.capitalize(), args.dst_tier.capitalize()
        blobs = self.get_blobs(args, src_tier, args.force)
        self.set_tag(blobs, dst_tier, args.excluded_indices_enabled)

    def _get_index_blobs(self):
        proc = subprocess.Popen(
            f'az storage blob list --account-name {self.account_name} '
            f'--container-name {self.container_name} --prefix \'stellar_data_backup/index-\' '
            f'--query \'[*].name\'',
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
        )
        out, err = proc.communicate()
        if proc.returncode != 0:
            err_str = err.decode()
            if self.is_throttled(err_str):
                return "", False
            else:
                raise BlobOperatorException(f'failed to list index blob: {err.decode()}')
        return json.loads(out), True

    def get_index_blobs(self):
        while True:
            result, ok = self._get_index_blobs()
            if ok:
                return result
            self.avoid_throttling(self.THROTTLING_SECONDS)

    def _download(self, name, filename, ignore_error=False):
        proc = subprocess.Popen(
            f'az storage blob download --account-name {self.account_name} '
            f'--container-name {self.container_name} --name {name} '
            f'--file {filename} --no-progress',
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
        )
        _, err = proc.communicate()
        if proc.returncode != 0:
            err_str = err.decode()
            if self.is_throttled(err_str):
                return False
            elif not ignore_error:
                raise BlobOperatorException(f'failed to download {name}: {err.decode()}')
        else:
            self.log(f'download file {name} to {filename}')
        return True

    def download(self, name, filename, ignore_error=False):
        while not self._download(name, filename, ignore_error=ignore_error):
            self.avoid_throttling(self.THROTTLING_SECONDS)

    def _upload(self, name, filename):
        proc = subprocess.Popen(
            f'az storage blob upload --account-name {self.account_name} '
            f'--container-name {self.container_name} --name {name} '
            f'--file {filename} --no-progress',
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
        )
        _, err = proc.communicate()
        if proc.returncode != 0:
            err_str = err.decode()
            if self.is_throttled(err_str):
                return False
            else:
                raise BlobOperatorException(f'failed to upload {name}: {err.decode()}')
        else:
            self.log(f'upload file {filename} to {name}')
        return True

    def upload(self, name, filename):
        while not self._upload(name, filename):
            self.avoid_throttling(self.THROTTLING_SECONDS)

    def get_prefix(self, args: argparse.Namespace):
        index_ids = self.do_get_prefix(args.names, self.get_index_blobs, self.download)
        if index_ids:
            print(f"get prefix of indices: {self.COMMON_INDEX_PREFIX}/\n")
            print(f"get id of indices: {index_ids}\n")
            print(f'get id of indices (for bash): {" ".join(index_ids)}')


def restore_factory(vendor):
    def restore(args):
        operator = BlobOperator.get_operator(vendor, args)
        operator.restore(args)
    return restore


def archive_factory(vendor):
    def archive(args):
        operator = BlobOperator.get_operator(vendor, args)
        operator.archive(args)
    return archive


def tag_factory(vendor):
    def tag(args):
        operator = BlobOperator.get_operator(vendor, args)
        operator.tag(args)
    return tag


def sync_factory(vendor):
    def sync(args):
        operator = BlobOperator.get_operator(vendor, args)
        operator.sync(args)
    return sync


def get_prefix_factory(vendor):
    def get_prefix(args):
        operator = BlobOperator.get_operator(vendor, args)
        operator.get_prefix(args)
    return get_prefix


def setup_aws_actions(parser):
    subparsers = parser.add_subparsers()

    parser_restore = subparsers.add_parser('restore')
    parser_restore.add_argument('--included-prefix', default='stellar_data_backup/indices/',
                                help='The included prefix. (default: stellar_data_backup/indices/)')
    parser_restore.add_argument('--restore-days', type=int, default=10,
                                help=f'The days of restore. (default: 10)')
    parser_restore.set_defaults(func=restore_factory(BlobOperator.VENDOR_AWS))

    parser_sync = subparsers.add_parser('sync')
    parser_sync.add_argument('--included-prefix', default='stellar_data_backup/indices/',
                             help='The included prefix. (default: stellar_data_backup/indices/)')
    parser_sync.add_argument('--no-excluded-indices', action='store_false',
                             dest='excluded_indices_enabled',
                             help='Skip using excluded indices file.')
    parser_sync.set_defaults(func=sync_factory(BlobOperator.VENDOR_AWS))

    parser_tag = subparsers.add_parser('tag')
    parser_tag.add_argument('--included-prefix', default='stellar_data_backup/indices/',
                            help='The included prefix. (default: stellar_data_backup/indices/)')
    parser_tag.add_argument('--no-excluded-indices', action='store_false',
                            dest='excluded_indices_enabled',
                            help='Skip using excluded indices file.')
    parser_tag.add_argument('--force', action='store_true', help='Ignore --src-tier option. (default: false)')
    parser_tag.add_argument('--src-tier', choices=(BlobOperator.BLOB_TIER_ARCHIVE, BlobOperator.BLOB_TIER_HOT),
                            default=BlobOperator.BLOB_TIER_HOT,
                            help=f'The source tier. (default: {BlobOperator.BLOB_TIER_HOT})')
    parser_tag.add_argument('--dst-tier', choices=(BlobOperator.BLOB_TIER_ARCHIVE, BlobOperator.BLOB_TIER_HOT),
                            default=BlobOperator.BLOB_TIER_ARCHIVE,
                            help=f'The destination tier. (default: {BlobOperator.BLOB_TIER_ARCHIVE})')
    parser_tag.set_defaults(func=tag_factory(BlobOperator.VENDOR_AWS))

    parser_get_prefix = subparsers.add_parser('get-prefix')
    parser_get_prefix.add_argument('names', help=f'The names of indices.')
    parser_get_prefix.set_defaults(func=get_prefix_factory(BlobOperator.VENDOR_AWS))


def setup_azure_actions(parser):
    subparsers = parser.add_subparsers()

    parser_restore = subparsers.add_parser('restore')
    parser_restore.add_argument('--included-prefix', default='stellar_data_backup/indices/',
                                help='The included prefix. (default: stellar_data_backup/indices/)')
    parser_restore.add_argument('--no-excluded-indices', action='store_false',
                                dest='excluded_indices_enabled',
                                help='Skip using excluded indices file.')
    parser_restore.set_defaults(func=restore_factory(BlobOperator.VENDOR_AZURE))

    parser_archive = subparsers.add_parser('archive')
    parser_archive.add_argument('--included-prefix', default='stellar_data_backup/indices/',
                                help='The included prefix. (default: stellar_data_backup/indices/)')
    parser_archive.set_defaults(func=archive_factory(BlobOperator.VENDOR_AZURE))

    parser_tag = subparsers.add_parser('tag')
    parser_tag.add_argument('--included-prefix', default='stellar_data_backup/indices/',
                            help='The included prefix. (default: stellar_data_backup/indices/)')
    parser_tag.add_argument('--force', action='store_true', help='Ignore --src-tier option. (default: false)')
    parser_tag.add_argument('--no-excluded-indices', action='store_false',
                            dest='excluded_indices_enabled',
                            help='Skip using excluded indices file.')
    parser_tag.add_argument('--src-tier', choices=(BlobOperator.BLOB_TIER_ARCHIVE, BlobOperator.BLOB_TIER_HOT),
                            default=BlobOperator.BLOB_TIER_HOT,
                            help=f'The source tier. (default: {BlobOperator.BLOB_TIER_HOT})')
    parser_tag.add_argument('--dst-tier', choices=(BlobOperator.BLOB_TIER_ARCHIVE, BlobOperator.BLOB_TIER_HOT),
                            default=BlobOperator.BLOB_TIER_ARCHIVE,
                            help=f'The destination tier. (default: {BlobOperator.BLOB_TIER_ARCHIVE})')
    parser_tag.set_defaults(func=tag_factory(BlobOperator.VENDOR_AZURE))

    parser_get_prefix = subparsers.add_parser('get-prefix')
    parser_get_prefix.add_argument('names', help=f'The names of indices.')
    parser_get_prefix.set_defaults(func=get_prefix_factory(BlobOperator.VENDOR_AZURE))


def parse_args():
    parser = argparse.ArgumentParser(description='Archive helpers.')
    parser.add_argument('--trace', dest='trace_enabled', action='store_true', help='Enable trace log. (default: false)')
    subparsers = parser.add_subparsers()

    # aws
    parser_aws = subparsers.add_parser('aws')
    parser_aws.add_argument('--bucket', required=True, help='The bucket name.')
    parser_aws.add_argument(
        '--page-size', type=int, default=1000,
        help='Request a smaller number of items from each call.'
    )
    setup_aws_actions(parser_aws)

    # azure
    parser_azure = subparsers.add_parser('azure')
    parser_azure.add_argument('--account-name', required=True, help='Storage account name.')
    parser_azure.add_argument('--container-name', required=True, help='The container name.')
    parser_azure.add_argument(
        '--excluded-prefix', default='',
        help='The excluded prefix. Exclusive between --included-prefix. Used with azure. (default: "")')
    parser_azure.add_argument(
        '--num-results', type=int, default=5000,
        help='Specify the maximum number to return. (default: 5000)')
    setup_azure_actions(parser_azure)

    return parser.parse_args()


if __name__ == "__main__":
    global_args = parse_args()
    global_args.func(global_args)
