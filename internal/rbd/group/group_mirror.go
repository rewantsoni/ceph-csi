/*
Copyright 2025 The Ceph-CSI Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package group

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ceph/go-ceph/rados"
	librbd "github.com/ceph/go-ceph/rbd"
	"github.com/ceph/go-ceph/rbd/admin"

	rbderrors "github.com/ceph/ceph-csi/internal/rbd/errors"
	"github.com/ceph/ceph-csi/internal/rbd/types"
	"github.com/ceph/ceph-csi/internal/util"
	"github.com/ceph/ceph-csi/internal/util/log"
)

type volumeGroupMirror volumeGroup

func (vgm *volumeGroupMirror) EnableMirroring(ctx context.Context, mode librbd.ImageMirrorMode) error {
	name, err := vgm.GetName(ctx)
	if err != nil {
		return err
	}

	ioctx, err := vgm.GetIOContext(ctx)
	if err != nil {
		return err
	}

	err = librbd.MirrorGroupEnable(ioctx, name, mode)
	if err != nil {
		return fmt.Errorf("failed to enable mirroring on volume group %q: %w", vgm, err)
	}

	log.DebugLog(ctx, "mirroring is enabled on the volume group %q", vgm)

	return nil
}

func (vgm *volumeGroupMirror) DisableMirroring(ctx context.Context, force bool) error {
	name, err := vgm.GetName(ctx)
	if err != nil {
		return err
	}

	ioctx, err := vgm.GetIOContext(ctx)
	if err != nil {
		return err
	}

	err = librbd.MirrorGroupDisable(ioctx, name, force)
	if err != nil && !errors.Is(err, rados.ErrNotFound) {
		return fmt.Errorf("failed to disable mirroring on volume group %q: %w", vgm, err)
	}

	log.DebugLog(ctx, "mirroring is disabled on the volume group %q", vgm)

	return nil
}

func (vgm *volumeGroupMirror) Promote(ctx context.Context, force bool) error {
	name, err := vgm.GetName(ctx)
	if err != nil {
		return err
	}

	ioctx, err := vgm.GetIOContext(ctx)
	if err != nil {
		return err
	}

	err = librbd.MirrorGroupPromote(ioctx, name, force)
	if err != nil {
		return fmt.Errorf("failed to promote volume group %q: %w", vgm, err)
	}

	log.DebugLog(ctx, "volume group %q has been promoted", vgm)

	return nil
}

func (vgm *volumeGroupMirror) ForcePromote(ctx context.Context, cr *util.Credentials) error {
	promoteArgs := []string{
		"mirror", "group", "promote",
		vgm.String(),
		"--force",
		"--id", cr.ID,
		"-m", vgm.monitors,
		"--keyfile=" + cr.KeyFile,
	}
	_, stderr, err := util.ExecCommandWithTimeout(
		ctx,
		// 2 minutes timeout as the Replication RPC timeout is 2.5 minutes.
		2*time.Minute,
		"rbd",
		promoteArgs...,
	)
	if err != nil {
		return fmt.Errorf("failed to promote group %q with error: %w", vgm, err)
	}

	if stderr != "" {
		return fmt.Errorf("failed to promote group %q with stderror: %s", vgm, stderr)
	}

	log.DebugLog(ctx, "volume group %q has been force promoted", vgm)

	return nil
}

func (vgm *volumeGroupMirror) Demote(ctx context.Context) error {
	name, err := vgm.GetName(ctx)
	if err != nil {
		return err
	}

	ioctx, err := vgm.GetIOContext(ctx)
	if err != nil {
		return err
	}

	err = librbd.MirrorGroupDemote(ioctx, name)
	if err != nil {
		return fmt.Errorf("failed to demote volume group %q: %w", vgm, err)
	}

	log.DebugLog(ctx, "volume group %q has been demoted", vgm)

	return nil
}

func (vgm *volumeGroupMirror) Resync(ctx context.Context) error {
	name, err := vgm.GetName(ctx)
	if err != nil {
		return err
	}

	ioctx, err := vgm.GetIOContext(ctx)
	if err != nil {
		return err
	}

	err = librbd.MirrorGroupResync(ioctx, name)
	if err != nil {
		return fmt.Errorf("failed to resync volume group %q: %w", vgm, err)
	}

	log.DebugLog(ctx, "issued resync on volume group %q", vgm)
	// If we issued a resync, return a non-final error as image needs to be recreated
	// locally. Caller retries till RBD syncs an initial version of the image to
	// report its status in the resync request.
	return fmt.Errorf("%w: awaiting initial resync due to split brain", rbderrors.ErrGroupUnavailable)
}

func (vgm *volumeGroupMirror) GetMirroringInfo(ctx context.Context) (types.MirrorInfo, error) {
	name, err := vgm.GetName(ctx)
	if err != nil {
		return nil, err
	}

	ioctx, err := vgm.GetIOContext(ctx)
	if err != nil {
		return nil, err
	}

	info, err := librbd.GetMirrorGroupInfo(ioctx, name)
	if err != nil {
		return nil, fmt.Errorf("failed to get volume group mirroring info %q: %w", vgm, err)
	}

	gi := groupInfo(*info)

	return gi, nil
}

func (vgm *volumeGroupMirror) GetGlobalMirroringStatus(ctx context.Context) (types.GlobalStatus, error) {
	name, err := vgm.GetName(ctx)
	if err != nil {
		return nil, err
	}

	ioctx, err := vgm.GetIOContext(ctx)
	if err != nil {
		return nil, err
	}
	statusInfo, err := librbd.GetGlobalMirrorGroupStatus(ioctx, name)
	if err != nil {
		return nil, fmt.Errorf("failed to get volume group mirroring status %q: %w", vgm, err)
	}

	gms := globalMirrorGroupStatus(statusInfo)

	return &gms, nil
}

func (vgm *volumeGroupMirror) AddSnapshotScheduling(interval admin.Interval, startTime admin.StartTime) error {
	ls := admin.NewLevelSpec(vgm.pool, vgm.namespace, "")
	ra, err := vgm.conn.GetRBDAdmin()
	if err != nil {
		return err
	}
	adminConn := ra.MirrorSnashotSchedule()
	err = adminConn.Add(ls, interval, startTime)
	if err != nil {
		return err
	}

	return nil
}

// groupInfo is a wrapper around librbd.MirrorGroupInfo that contains the
// group mirror info.
type groupInfo librbd.MirrorGroupInfo

func (info groupInfo) GetState() string {
	return info.State.String()
}

func (info groupInfo) IsPrimary() bool {
	return info.Primary
}

// globalMirrorGroupStatus is a wrapper around librbd.GlobalGroupMirrorImageStatus that contains the
// global mirror group status.
type globalMirrorGroupStatus librbd.GlobalMirrorGroupStatus

func (status *globalMirrorGroupStatus) GetState() string {
	return status.Info.State.String()
}

func (status *globalMirrorGroupStatus) IsPrimary() bool {
	return status.Info.Primary
}

func (status *globalMirrorGroupStatus) GetLocalSiteStatus() (types.SiteStatus, error) {
	mstatus := librbd.GlobalMirrorGroupStatus(*status)
	s, err := mstatus.LocalStatus()
	if err != nil {
		err = fmt.Errorf("failed to get local site status: %w", err)
	}

	smgs := siteMirrorGroupStatus(s)

	return &smgs, err
}

func (status *globalMirrorGroupStatus) GetAllSitesStatus() []types.SiteStatus {
	var siteStatuses []types.SiteStatus
	for _, ss := range status.SiteStatuses {
		smgs := siteMirrorGroupStatus(ss)
		siteStatuses = append(siteStatuses, &smgs)
	}

	return siteStatuses
}

// RemoteStatus returns one SiteMirrorGroupStatus item from the SiteStatuses
// slice that corresponds to the remote site's status. If the remote status
// is not found than the error ErrStatusNotFound will be returned.
func (status *globalMirrorGroupStatus) GetRemoteSiteStatus(ctx context.Context) (types.SiteStatus, error) {
	var (
		ss  librbd.SiteMirrorGroupStatus
		err error = rbderrors.ErrStatusNotFound
	)

	for i := range status.SiteStatuses {
		log.DebugLog(
			ctx,
			"Site status of MirrorUUID: %s, state: %s, description: %s, lastUpdate: %v, up: %t",
			status.SiteStatuses[i].MirrorUUID,
			status.SiteStatuses[i].State,
			status.SiteStatuses[i].Description,
			status.SiteStatuses[i].LastUpdate,
			status.SiteStatuses[i].Up)

		if status.SiteStatuses[i].MirrorUUID != "" {
			ss = status.SiteStatuses[i]
			err = nil

			break
		}
	}

	grmss := siteMirrorGroupStatus(ss)

	return &grmss, err
}

// siteMirrorGroupStatus is a wrapper around librbd.SiteMirrorGroupStatus that contains the
// site mirror group status.
type siteMirrorGroupStatus librbd.SiteMirrorGroupStatus

func (status *siteMirrorGroupStatus) GetMirrorUUID() string {
	return status.MirrorUUID
}

func (status *siteMirrorGroupStatus) GetState() string {
	return status.State.String()
}

func (status *siteMirrorGroupStatus) GetDescription() string {
	return status.Description
}

func (status *siteMirrorGroupStatus) IsUP() bool {
	return status.Up
}

func (status *siteMirrorGroupStatus) GetLastUpdate() time.Time {
	// convert the last update time to UTC
	return time.Unix(status.LastUpdate, 0).UTC()
}
