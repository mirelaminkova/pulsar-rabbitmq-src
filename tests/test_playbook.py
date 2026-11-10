"""Structure of the playbook, parameters and templates: secrets, ordering, removed relay code."""

import html
import re
import unittest

from tests.helpers import PASSWORD, REPO, load_yaml, render, resolve


def tasks(path):
    """All tasks of a task list, including those inside blocks."""
    result = []
    for task in load_yaml(path) or []:
        result.append(task)
        result.extend(task.get("block", []))
    return result


def play():
    [play] = load_yaml("playbook.yml")
    return play


def task_named(task_list, name):
    [task] = [t for t in task_list if t.get("name") == name]
    return task


class RemovedRelayTest(unittest.TestCase):
    def test_relay_valkey_and_admin_files_are_gone(self):
        for path in (
            "relay_src",
            "Dockerfile",
            "compose.yml",
            ".dockerignore",
            "files/valkey.conf",
            "scripts",
            "templates/admin-location.conf.j2",
            "templates/admin-sync.service.j2",
            "templates/admin-sync.timer.j2",
        ):
            with self.subTest(path=path):
                self.assertFalse((REPO / path).exists())

    def test_no_broker_on_this_workspace(self):
        files = [
            REPO / "playbook.yml",
            REPO / "parameters.yml",
            REPO / "group_vars" / "all.yml",
            *(REPO / "tasks").glob("*.yml"),
            *(REPO / "templates").iterdir(),
        ]
        for path in files:
            with self.subTest(path=path.name):
                text = path.read_text().lower()
                found = [
                    w
                    for w in ("valkey", "rabbitmq-server", "docker_compose", "admin-sync")
                    if w in text
                ]
                self.assertEqual(found, [])


class ParametersTest(unittest.TestCase):
    def test_component_parameters_map_onto_galaxy(self):
        parameters = load_yaml("parameters.yml")

        for name in (
            "pulsar_galaxy_fqdn",
            "pulsar_amqp_port",
            "pulsar_amqp_name",
            "pulsar_amqp_password",
            "pulsar_manager",
            "pulsar_staging_dir",
            "pulsar_cvmfs",
            "pulsar_docker",
            "pulsar_apptainer",
        ):
            self.assertIn(name, parameters)
        self.assertEqual(parameters["pulsar_amqp_password"], "")

    def test_parameters_have_group_vars_defaults(self):
        group_vars = load_yaml("group_vars/all.yml")
        self.assertLessEqual(set(load_yaml("parameters.yml")), set(group_vars))

    def test_parameter_defaults_agree_with_group_vars(self):
        # SRC passes parameters.yml's strings; without SRC, group_vars apply. Same result.
        group_vars = load_yaml("group_vars/all.yml")
        parameters = {
            key: value
            for key, value in load_yaml("parameters.yml").items()
            if key not in ("pulsar_galaxy_fqdn", "pulsar_amqp_name", "pulsar_amqp_password")
        }
        defaults = {key: group_vars[key] for key in parameters}
        for name in ("_pulsar_amqp", "_pulsar_cvmfs"):
            with self.subTest(name=name):
                self.assertEqual(resolve(name, **parameters), resolve(name, **defaults))


class PlaybookTest(unittest.TestCase):
    def test_restart_survives_a_later_failure(self):
        # A failing task after a changed app.yml must still restart Pulsar.
        self.assertIs(play().get("force_handlers"), True)

    def test_validation_comes_first_and_never_shows_values(self):
        pre_tasks = play()["pre_tasks"]
        check = pre_tasks[0]["ansible.builtin.assert"]

        self.assertEqual(check["that"], "_pulsar_amqp_errors | length == 0")
        self.assertIn("_pulsar_amqp_errors", check["fail_msg"])
        self.assertNotIn("password", check["fail_msg"])
        self.assertTrue(pre_tasks[0].get("no_log") is not True)  # the message must be visible

    def test_tasks_with_secrets_are_not_logged(self):
        play_tasks = [t for task in play()["tasks"] for t in [task, *task.get("block", [])]]
        for task in play_tasks + tasks("tasks/pulsar.yml"):
            text = str(task)
            if "block" not in task and ("pulsar-app.yml.j2" in text or "welcome.html.j2" in text):
                with self.subTest(task=task["name"]):
                    self.assertIs(task.get("no_log"), True)
                    self.assertIs(task.get("diff"), False)

    def test_app_yml_is_readable_by_pulsar_only(self):
        task = task_named(
            tasks("tasks/pulsar.yml"), "Write Pulsar's app.yml with the Galaxy broker connection"
        )
        module = task["ansible.builtin.template"]

        self.assertEqual(
            (module["owner"], module["group"], module["mode"]),
            ("root", "{{ pulsar_user }}", "0640"),
        )

    def test_pulsar_installed_with_amqp_support(self):
        task = task_named(
            tasks("tasks/pulsar.yml"), "Install pulsar-app with AMQP support into the venv"
        )

        self.assertIn("pulsar-app[amqp]=={{ pulsar_version }}", task["ansible.builtin.pip"]["name"])
        self.assertEqual(resolve("pulsar_version"), "0.15.15")

    def test_connection_check_after_restart_and_fails_clearly(self):
        names = [t.get("name") for t in tasks("tasks/pulsar.yml")]
        check = task_named(
            tasks("tasks/pulsar.yml"),
            "Check that Pulsar consumes its setup queue on Galaxy's broker",
        )
        report = task_named(
            tasks("tasks/pulsar.yml"), "Fail when Pulsar cannot use Galaxy's broker"
        )

        self.assertLess(
            names.index("Apply pending Pulsar restarts before checking the connection"),
            names.index(check["name"]),
        )
        self.assertEqual(names.index(report["name"]), names.index(check["name"]) + 1)
        argv = check["ansible.builtin.command"]["argv"]
        self.assertEqual(argv[0], "{{ pulsar_venv }}/bin/python")
        self.assertEqual(argv[1:], ["{{ pulsar_check_script }}", "{{ pulsar_config_dir }}/app.yml"])
        self.assertEqual(check["until"], "_pulsar_amqp_check.rc != 75")  # check_amqp.RETRY
        self.assertIn("_pulsar_amqp_check.stdout", report["ansible.builtin.fail"]["msg"])
        self.assertEqual(report["when"], "_pulsar_amqp_check.rc != 0")


class TemplatesTest(unittest.TestCase):
    def test_welcome_page_shows_the_galaxy_entry(self):
        page = render("welcome.html.j2")
        entry = resolve("_pulsar_amqp")["galaxy_entry"]

        self.assertIn(f'<pre id="galaxy-amqp-param">{html.escape(entry)}</pre>', page)
        self.assertIn("src_galaxy_pulsar_amqp_configs", page)
        self.assertIn("environment: amqp_hpc", page)
        self.assertIn("src_galaxy_rabbitmq", page)
        # Galaxy's preferences list the entry by its label, which defaults to the name.
        self.assertIn("choosing <code>hpc</code>", page)

    def test_welcome_page_does_not_claim_a_check_that_may_have_failed(self):
        # The page is published before the broker check, so it must not report its result.
        self.assertNotIn("checked that", render("welcome.html.j2"))

    def test_welcome_page_escapes_values(self):
        page = render("welcome.html.j2", ansible_hostname="<b>x</b>")
        self.assertNotIn("<b>x</b>", page)

    def test_welcome_page_contains_the_password_only_in_the_entry(self):
        page = render("welcome.html.j2")
        self.assertEqual(page.count(PASSWORD), 1)

    def test_service_runs_pulsar_main_as_pulsar(self):
        unit = render("pulsar.service.j2")

        self.assertIn("User=pulsar", unit)
        self.assertRegex(unit, re.compile(r"^ExecStart=/opt/pulsar/venv/bin/pulsar-main ", re.M))


if __name__ == "__main__":
    unittest.main()
