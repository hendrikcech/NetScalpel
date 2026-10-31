package main

import (
	"fmt"
	"path/filepath"
)

type namedOutputPath struct {
	flag string
	path string
}

func validateOutputPathCollisions(out, logPath, info string) error {
	// shortcut: linked output aliases are not detected, add inode checks if linked outputs are needed.
	paths := []namedOutputPath{
		{flag: "--out", path: out},
		{flag: "--log", path: logPath},
		{flag: "--info", path: info},
	}
	for i := range paths {
		if paths[i].path == "" {
			continue
		}
		for j := i + 1; j < len(paths); j++ {
			if paths[j].path == "" {
				continue
			}
			same, err := sameOutputTarget(paths[i].path, paths[j].path)
			if err != nil {
				return fmt.Errorf("resolve %s and %s paths: %w", paths[i].flag, paths[j].flag, err)
			}
			if same {
				return fmt.Errorf("%s and %s refer to the same output path", paths[i].flag, paths[j].flag)
			}
		}
	}
	return nil
}

func sameOutputTarget(a, b string) (bool, error) {
	canonicalA, err := canonicalOutputPath(a)
	if err != nil {
		return false, err
	}
	canonicalB, err := canonicalOutputPath(b)
	if err != nil {
		return false, err
	}
	return canonicalA == canonicalB, nil
}

func canonicalOutputPath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.Clean(absolute), nil
}
