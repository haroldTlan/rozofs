/*
  Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
  This file is part of Rozofs.

  Rozofs is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published
  by the Free Software Foundation; either version 3 of the License,
  or (at your option) any later version.

  Rozofs is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see
  <http://www.gnu.org/licenses/>.
 */
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include "config.h"
#include "list.h"
#include "log.h"
#include "monitor.h"

#define HEADER "\
# This file was generated by exportd(8) version: %s.\n\
# All changes to this file will be lost.\n\n"

int monitor_initialize() {
    int status = -1;
    char path[FILENAME_MAX];
    DEBUG_FUNCTION;

    sprintf(path, "%s%s", DAEMON_PID_DIRECTORY, "exportd");
    if (access(path, X_OK) != 0) {
        if (mkdir(path, S_IRWXU) != 0) {
            severe("can't create %s", path);
            goto out;
        }
    }
    status = 0;
out:
    return status;
}

void monitor_release() {
    //XXX should clean MONITOR_DIRECTORY
    return;
}

int monitor_volume(volume_t *volume) {
    int status = -1;
    int fd = -1;
    char path[FILENAME_MAX];
    volume_stat_t vstat;
    list_t *p, *q;
    DEBUG_FUNCTION;

    sprintf(path, "%s%s%d", DAEMON_PID_DIRECTORY, "exportd/volume_", volume->vid);
    if ((fd = open(path, O_WRONLY|O_CREAT)) < 0) {
        severe("can't open %s", path);
        goto out;
    }

    dprintf(fd, HEADER, VERSION);
    dprintf(fd, "volume: %d\n", volume->vid);

    volume_stat(volume, &vstat);
    dprintf(fd, "bsize: %d\n", vstat.bsize);
    dprintf(fd, "bfree: %lu\n", vstat.bfree);
    dprintf(fd, "nb_clusters: %d\n", list_size(&volume->clusters));
    list_for_each_forward(p, &volume->clusters) {
        cluster_t *cluster = list_entry(p, cluster_t, list);
        dprintf(fd, "cluster: %d\n", cluster->cid);
        dprintf(fd, "nb_storages: %d\n", list_size(&cluster->storages));
        dprintf(fd, "size: %lu\n", cluster->size);
        dprintf(fd, "free: %lu\n", cluster->free);
        list_for_each_forward(q, &cluster->storages) {
            volume_storage_t *storage = list_entry(q, volume_storage_t, list);
            dprintf(fd, "storage: %d\n", storage->sid);
            dprintf(fd, "host: %s\n", storage->host);
            dprintf(fd, "status: %d\n", storage->status);
            dprintf(fd, "size: %lu\n", storage->stat.size);
            dprintf(fd, "free: %lu\n", storage->stat.free);
        }
    }
    status = 0;
out:
    if (fd > 0) close(fd);
    return status;
}

int monitor_export(export_t *export) {
    int status = -1;
    int fd = -1;
    char path[FILENAME_MAX];
    estat_t estat;
    uint64_t exceed = 0;
    DEBUG_FUNCTION;

    sprintf(path, "%s%s%d", DAEMON_PID_DIRECTORY, "exportd/export_", export->eid);
    if ((fd = open(path, O_WRONLY|O_CREAT|O_TRUNC)) < 0) {
        severe("can't open %s", path);
        goto out;
    }

    if (export_stat(export, &estat) != 0) {
        severe("can't stat export: %d", export->eid);
        goto out;
    }

    dprintf(fd, HEADER, VERSION);
    dprintf(fd, "export: %d\n", export->eid);
    dprintf(fd, "volume: %d\n", export->volume->vid);
    dprintf(fd, "root: %s\n", export->root);
    dprintf(fd, "squota: %lu\n", export->squota);
    dprintf(fd, "hquota: %lu\n", export->hquota);
    dprintf(fd, "bsize: %d\n", estat.bsize);
    dprintf(fd, "blocks: %lu\n", estat.blocks);
    dprintf(fd, "bfree: %lu\n", estat.bfree);
    dprintf(fd, "files: %lu\n", estat.files);
    dprintf(fd, "ffree: %lu\n", estat.ffree);
    if (export->squota > 0) {
        exceed = estat.blocks - estat.bfree > export->squota ?
            estat.blocks - estat.bfree - export->squota : 0;
    }
    dprintf(fd, "squota_exceed: %lu\n", exceed);

    status = 0;
out:
    if (fd > 0) close(fd);
    return status;
}