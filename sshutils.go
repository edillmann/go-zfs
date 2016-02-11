package zfs

import (
	"fmt"
	"strings"
	"golang.org/x/crypto/ssh"
	"io/ioutil"
)

func (cmd *command) StartCommand() (error, *ssh.Session) {
	var (
		session *ssh.Session
		err error
	)

	z := cmd.zh

	// open ssh link
	if (z.client == nil) {
		if err = z.dialSSH(); err != nil {
			return err, nil
		}
	}

	// establish ssh session
	if session, err = z.client.NewSession(); err != nil {
		return err, nil
	}

	// setup env, stdin, stdout, stderr
	if err = prepareCommand(session, cmd); err != nil {
		return err, nil
	}

	// start remote command
	err = session.Start(cmd.Path)
	if err == nil {
		return err, session
	}
	return err, nil
}

func prepareCommand(session *ssh.Session, cmd *command) error {
	for _, env := range cmd.Env {
		variable := strings.Split(env, "=")
		if len(variable) != 2 {
			continue
		}

		if err := session.Setenv(variable[0], variable[1]); err != nil {
			return err
		}
	}

	if cmd.Stdout == nil {
		session.Stdout = &cmd.stdout
	} else {
		session.Stdout = cmd.Stdout
	}

	if cmd.Stdin != nil {
		session.Stdin = cmd.Stdin

	}
	if cmd.Stderr == nil {
		session.Stderr = &cmd.stderr
	} else {
		session.Stderr = cmd.Stderr
	}
	return nil
}

func getKeyFile(keyfile string) (key ssh.Signer, err error) {
	buf, err := ioutil.ReadFile(keyfile)
	if err != nil {
		return
	}
	key, err = ssh.ParsePrivateKey(buf)
	if err != nil {
		return
	}
	return
}

func (z *ZfsH) dialSSH() error {

	// keyfile authentifcation
	key, err := getKeyFile(z.keyfile);
	if err != nil {
		panic(err)
	}
	sshConfig := &ssh.ClientConfig{
		User: z.username,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(key),
		},
	}

	// password authentication
	if z.password != "" {
		sshConfig.Auth = append(sshConfig.Auth, ssh.Password(z.password))
	}

	z.client, err = ssh.Dial("tcp", fmt.Sprintf("%s:%d", z.host, z.port), sshConfig)
	if err != nil {
		return fmt.Errorf("Failed to dial: %s", err)
	}
	return nil
}
