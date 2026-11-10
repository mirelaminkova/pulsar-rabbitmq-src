"""Component parameters: defaults, validation and the Galaxy entry they produce.

Validation must accept exactly what Galaxy's parser accepts for the same values
(vendored in tests/galaxy_contract/src_galaxy.py), so a deployment that passes
here cannot be rejected by the Galaxy component, and vice versa, apart from the
documented stricter rules for the staging directory and the Galaxy hostname.
"""

import unittest

from filter_plugins.pulsar_amqp import pulsar_amqp_errors, pulsar_amqp_settings
from tests.galaxy_contract import src_galaxy
from tests.helpers import PASSWORD, resolve

BASE = {
    "galaxy_fqdn": "galaxy.example.org",
    "port": "5671",
    "name": "hpc",
    "password": PASSWORD,
    "manager": "_default_",
    "jobs_directory": "/mnt/pulsar/files/staging",
    "docker_enabled": "true",
    "singularity_enabled": "true",
}


def settings(**overrides):
    return pulsar_amqp_settings({**BASE, **overrides})


def galaxy_errors(entry_text):
    entries = src_galaxy.src_config_list(entry_text, "src_galaxy_pulsar_amqp_configs")
    return src_galaxy.pulsar_amqp_config_errors(entries)


class DefaultsTest(unittest.TestCase):
    def test_unset_parameters_get_galaxys_defaults(self):
        # SRC passes an unset parameter as an empty string.
        result = settings(port="", manager="", jobs_directory="  ")

        self.assertEqual(result["port"], 5671)
        self.assertEqual(result["manager"], src_galaxy.AMQP_DEFAULTS["manager"])
        self.assertEqual(result["jobs_directory"], src_galaxy.AMQP_DEFAULTS["jobs_directory"])
        self.assertEqual(pulsar_amqp_errors(result), [])

    def test_derived_names_follow_galaxy(self):
        result = settings(name="eu-hpc")

        [config] = src_galaxy.pulsar_amqp_configs([{"name": "eu-hpc"}])
        self.assertEqual(result["vhost"], config["vhost"])
        self.assertEqual(result["environment"], config["environment"])
        self.assertEqual(result["tag"], "eu-hpc-pulsar")

    def test_url_has_no_ssl_query(self):
        # ?ssl=1 would replace Pulsar's amqp_connect_ssl_* settings (see test_pulsar_compat).
        self.assertEqual(
            settings()["url"],
            f"pyamqp://hpc:{PASSWORD}@galaxy.example.org:5671//pulsar/hpc",
        )

    def test_values_are_trimmed(self):
        result = settings(galaxy_fqdn=" Galaxy.Example.org ", name=" hpc ")

        self.assertEqual(result["galaxy_fqdn"], "galaxy.example.org")
        self.assertEqual(result["name"], "hpc")


class ValidationTest(unittest.TestCase):
    def assertInvalid(self, message_part, **overrides):
        errors = pulsar_amqp_errors(settings(**overrides))
        self.assertTrue(any(message_part in error for error in errors), errors)

    def test_valid(self):
        self.assertEqual(pulsar_amqp_errors(settings()), [])

    def test_required_values(self):
        errors = pulsar_amqp_errors(settings(galaxy_fqdn="", name="", password=""))

        self.assertEqual(len(errors), 3, errors)
        self.assertTrue(all("required" in error for error in errors), errors)

    def test_galaxy_fqdn_must_be_a_hostname(self):
        for value in (
            "https://galaxy.example.org",
            "galaxy.example.org/",
            "galaxy",
            "galaxy.example.org:5671",
            "-x.example.org",
            "a b.example.org",
        ):
            with self.subTest(value=value):
                self.assertInvalid("pulsar_galaxy_fqdn", galaxy_fqdn=value)

    def test_port(self):
        # Galaxy also refuses the ports RabbitMQ, Nginx and Galaxy use themselves.
        for value in (
            "0",
            "65536",
            "56x",
            "-1",
            "5671.0",
            "5672",
            "4369",
            "25672",
            "80",
            "443",
            "8080",
        ):
            with self.subTest(value=value):
                self.assertInvalid("pulsar_amqp_port", port=value)
        self.assertEqual(pulsar_amqp_errors(settings(port="15671")), [])

    def test_password_is_never_echoed(self):
        for value in (
            "short",
            "has space in it",
            "with,comma1234",
            "quote'1234567890",
            "colon:1234567890",
        ):
            with self.subTest(value=value):
                errors = pulsar_amqp_errors(settings(password=value))
                self.assertEqual(len(errors), 1, errors)
                self.assertNotIn(value, " ".join(errors))

    def test_staging_directory_is_stricter_than_galaxy(self):
        # Galaxy accepts any absolute path without spaces; Pulsar would own it, so
        # system directories and characters the SRC one-line format breaks on are refused.
        # Only below /mnt or /data (SRC storage), at least three levels deep, so neither a
        # system directory nor Pulsar's own installation can become the pulsar user's.
        for value in (
            "/",
            "/mnt",
            "/mnt/pulsar",
            "/data/volume_2",
            "/var/lib",
            "/opt/pulsar",
            "/opt/pulsar/config",
            "/var/lib/pulsar/x",
            "/etc/pulsar/staging",
            "/mnt/../etc/x",
            "/mnt/a/../b",
            "/mnt/./a/b",
            "/mnt/a/b,c",
            "/mnt/a/{b}",
            "relative/dir/x",
        ):
            with self.subTest(value=value):
                self.assertInvalid("pulsar_staging_dir", jobs_directory=value)
        for value in ("/mnt/pulsar/files/staging", "/data/volume_2/staging"):
            with self.subTest(value=value):
                self.assertEqual(pulsar_amqp_errors(settings(jobs_directory=value)), [])

    def test_booleans(self):
        self.assertFalse(settings(docker_enabled="false")["docker_enabled"])
        self.assertTrue(settings(docker_enabled=True)["docker_enabled"])
        self.assertInvalid("docker", docker_enabled="maybe")


class SameRulesAsGalaxyTest(unittest.TestCase):
    """For name, password and manager: valid here <=> Galaxy accepts our entry."""

    CASES = {  # noqa: RUF012 - read only
        "name": [
            "hpc",
            "eu_hpc-2",
            "a" * 64,
            "a" * 65,
            "HPC",
            "hp c",
            "galaxy",
            "guest",
            "true",
            "false",
            "hpc.1",
        ],
        "password": [
            PASSWORD,
            "abcdefghijkl",
            "abcdefghijk",
            "a~b.c_d-e123",
            "abc+defghijkl",
            "abc@defghijkl",
            "abc/defghijkl",
            "true" * 3,
            "x" * 200,
        ],
        "manager": ["_default_", "gpu", "Big-Queue_1", "a.b", "true", "with space"],
    }

    def test_rules_match_galaxys_parser(self):
        for key, values in self.CASES.items():
            for value in values:
                with self.subTest(key=key, value=value):
                    result = settings(**{key: value})
                    ours = [e for e in pulsar_amqp_errors(result) if key in e or "required" in e]
                    theirs = galaxy_errors(result["galaxy_entry"])
                    self.assertEqual(ours == [], theirs == [], (ours, theirs))


class GalaxyEntryTest(unittest.TestCase):
    def parsed(self, result):
        [entry] = src_galaxy.src_config_list(
            result["galaxy_entry"], "src_galaxy_pulsar_amqp_configs"
        )
        return entry

    def test_galaxy_parses_the_entry_without_errors(self):
        result = settings()

        self.assertEqual(galaxy_errors(result["galaxy_entry"]), [])
        self.assertLessEqual(set(self.parsed(result)), src_galaxy.AMQP_KEYS)

    def test_galaxy_derives_the_same_connection(self):
        result = settings(
            manager="gpu",
            jobs_directory="/data/volume_2/staging",
            docker_enabled="false",
            singularity_enabled="true",
        )
        [config] = src_galaxy.pulsar_amqp_configs([self.parsed(result)])

        self.assertEqual(config["name"], "hpc")
        self.assertEqual(config["password"], PASSWORD)
        self.assertEqual(config["vhost"], result["vhost"])
        self.assertEqual(config["manager"], "gpu")
        self.assertEqual(config["jobs_directory"], "/data/volume_2/staging")
        self.assertIs(config["singularity_enabled"], True)
        self.assertIs(config["docker_enabled"], False)

    def test_one_container_type(self):
        # With both, Galaxy would prefer Docker; Apptainer uses the CVMFS images and binds.
        for docker, singularity, expected in (
            ("true", "true", "singularity_enabled"),
            ("true", "false", "docker_enabled"),
            ("false", "true", "singularity_enabled"),
        ):
            with self.subTest(docker=docker, singularity=singularity):
                entry = self.parsed(
                    settings(docker_enabled=docker, singularity_enabled=singularity)
                )
                self.assertEqual({key for key in entry if key.endswith("_enabled")}, {expected})
        entry = self.parsed(settings(docker_enabled="false", singularity_enabled="false"))
        self.assertFalse({key for key in entry if key.endswith("_enabled")})

    def test_one_line(self):
        self.assertNotIn("\n", settings()["galaxy_entry"])


class GroupVarsTest(unittest.TestCase):
    def test_group_vars_build_the_settings_from_the_component_parameters(self):
        result = resolve("_pulsar_amqp")

        self.assertEqual(
            result["url"], f"pyamqp://hpc:{PASSWORD}@galaxy.example.org:5671//pulsar/hpc"
        )
        self.assertEqual(result["jobs_directory"], "/mnt/pulsar/files/staging")
        self.assertEqual(resolve("_pulsar_amqp_errors"), [])

    def test_group_vars_report_missing_parameters(self):
        errors = resolve("_pulsar_amqp_errors", pulsar_galaxy_fqdn="", pulsar_amqp_password="")

        self.assertEqual(len(errors), 2, errors)

    def test_empty_staging_parameter_keeps_the_default(self):
        self.assertEqual(
            resolve("_pulsar_amqp", pulsar_staging_dir="")["jobs_directory"],
            "/mnt/pulsar/files/staging",
        )


if __name__ == "__main__":
    unittest.main()
