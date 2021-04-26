package main

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/kennygrant/sanitize"
	"github.com/spf13/cobra"
)

// Backup is used to gather all of a container's metadata, so we can encode it
// as JSON and store it
type Backup struct {
	Config  *container.Config
	PortMap nat.PortMap
	Mounts  []types.MountPoint
}

var (
	optLaunch  = ""
	optTar     = false
	optAll     = false
	optStopped = false
	optExclude = []string{}

	paths []string
	tw    *tar.Writer

	backupCmd = &cobra.Command{
		Use:   "backup [container-id]",
		Short: "creates a backup of a container",
		RunE: func(cmd *cobra.Command, args []string) error {
			if optAll {
				return backupAll()
			}

			if len(args) < 1 {
				return fmt.Errorf("backup requires the ID of a container")
			}
			return backup(args[0])
		},
	}
)

func collectFile(path string, info os.FileInfo, err error) error {
	if err != nil {
		return err
	}

	paths = append(paths, path)
	return nil
}

func collectFileTar(path string, info os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSocket != 0 {
		// ignore sockets
		return nil
	}

	fmt.Println("Adding", path)

	th, err := tar.FileInfoHeader(info, path)
	if err != nil {
		return err
	}

	th.Name = path
	if si, ok := info.Sys().(*syscall.Stat_t); ok {
		th.Uid = int(si.Uid)
		th.Gid = int(si.Gid)
	}

	if err := tw.WriteHeader(th); err != nil {
		return err
	}

	if !info.Mode().IsRegular() {
		return nil
	}
	if info.Mode().IsDir() {
		return nil
	}

	file, err := os.Open(path)
	if err != nil {
		return err
	}

	_, err = io.Copy(tw, file)
	return err
}

func backupTar(filename string, backup Backup, binds []types.MountPoint) error {
	b, err := json.MarshalIndent(backup, "", "  ")
	if err != nil {
		return err
	}
	// fmt.Println(string(b))

	tarfile, err := os.Create(filename + ".tar")
	if err != nil {
		return err
	}
	tw = tar.NewWriter(tarfile)

	th := &tar.Header{
		Name:       "container.json",
		Size:       int64(len(b)),
		ModTime:    time.Now(),
		AccessTime: time.Now(),
		ChangeTime: time.Now(),
		Mode:       0600,
	}

	if err := tw.WriteHeader(th); err != nil {
		return err
	}
	if _, err := tw.Write(b); err != nil {
		return err
	}

	for _, m := range backup.Mounts {
		// fmt.Printf("Mount (type %s) %s -> %s\n", m.Type, m.Source, m.Destination)

		err := filepath.Walk(m.Source, collectFileTar)
		if err != nil {
			return err
		}
	}

	for _, m := range binds {
		// fmt.Printf("Mount (type %s) %s -> %s\n", m.Type, m.Source, m.Destination)

		err := filepath.Walk(m.Source, collectFileTar)
		if err != nil {
			return err
		}
	}

	tw.Close()
	fmt.Println("Created backup:", filename+".tar")
	return nil
}

func backup(ID string) error {
	conf, err := cli.ContainerInspect(ctx, ID)
	if err != nil {
		return err
	}
	fmt.Printf("Creating backup of %s (%s, %s)\n", conf.Name[1:], conf.Config.Image, conf.ID[:12])

	var mounts []types.MountPoint
	var backupFiles []types.MountPoint

	for _, mount := range conf.Mounts {
		if mount.Type == "bind" {
			skip := false
			for _, exclude := range optExclude {
				if strings.HasPrefix(mount.Source, exclude) {
					skip = true
					break
				}
			}

			if !skip {
				fmt.Println("Backup bind files:", mount.Source)
				backupFiles = append(backupFiles, mount)
			}
		} else {
			fmt.Println("Backup volume files:", mount.Source)
			mounts = append(mounts, mount)
			backupFiles = append(backupFiles, mount)
		}
	}

	backup := Backup{
		PortMap: conf.HostConfig.PortBindings,
		Config:  conf.Config,
		Mounts:  mounts,
	}

	filename := sanitize.Path(fmt.Sprintf("%s-%s", conf.Config.Image, ID))
	filename = strings.Replace(filename, "/", "_", -1)
	if optTar {
		return backupTar(filename, backup, backupFiles)
	}

	b, err := json.MarshalIndent(backup, "", "  ")
	if err != nil {
		return err
	}
	// fmt.Println(string(b))

	err = ioutil.WriteFile(filename+".backup.json", b, 0600)
	if err != nil {
		return err
	}

    paths = []string{}
	for _, m := range backupFiles {
		// fmt.Printf("Mount (type %s) %s -> %s\n", m.Type, m.Source, m.Destination)
		err := filepath.Walk(m.Source, collectFile)
		if err != nil {
			return err
		}
	}

	filelist, err := os.Create(filename + ".backup.files")
	if err != nil {
		return err
	}
	defer filelist.Close()

	_, err = filelist.WriteString(filename + ".backup.json\n")
	if err != nil {
		return err
	}
	for _, s := range paths {
		_, err := filelist.WriteString(s + "\n")
		if err != nil {
			return err
		}
	}

	fmt.Println("Created backup:", filename+".backup.json")

	if optLaunch != "" {
		ol := strings.Replace(optLaunch, "%tag", filename, -1)
		ol = strings.Replace(ol, "%list", filename+".backup.files", -1)

		fmt.Println("Launching external command and waiting for it to finish:")
		fmt.Println(ol)

		l := strings.Split(ol, " ")
		cmd := exec.Command(l[0], l[1:]...)
		return cmd.Run()
	}

	return nil
}

func backupAll() error {
	containers, err := cli.ContainerList(ctx, types.ContainerListOptions{
		All: optStopped,
	})
	if err != nil {
		panic(err)
	}

	for _, container := range containers {
		err := backup(container.ID)
		if err != nil {
			return err
		}
	}

	return nil
}

func init() {
	backupCmd.Flags().StringVarP(&optLaunch, "launch", "l", "", "launch external program with file-list as argument")
	backupCmd.Flags().BoolVarP(&optTar, "tar", "t", false, "create tar backups")
	backupCmd.Flags().BoolVarP(&optAll, "all", "a", false, "backup all running containers")
	backupCmd.Flags().BoolVarP(&optStopped, "stopped", "s", false, "in combination with --all: also backup stopped containers")
	backupCmd.Flags().StringArrayVarP(&optExclude, "exclude", "e", []string{}, "exclude file paths that start with this, can use multiple times")
	RootCmd.AddCommand(backupCmd)
}
