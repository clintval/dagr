import sbt._
import sbt.Keys._
import sbtassembly.AssemblyKeys.assembly
import sbtassembly.MergeStrategy
import com.typesafe.sbt.SbtGit.GitCommand
import scoverage.ScoverageSbtPlugin.ScoverageKeys._
import sbtrelease._
// we hide the existing definition for setReleaseVersion to replace it with our own
import sbtrelease.ReleaseStateTransformations.{setReleaseVersion=>_,_}

////////////////////////////////////////////////////////////////////////////////////////////////
// We have the following "settings" in this build.sbt:
// - custom versioning with sbt-release and sbt-git
// - custom JAR name for the root project
// - settings to publish to Sonatype
// - exclude the root, tasks, and pipelines project from code coverage
// - scaladoc settings
// - custom merge strategy for assembly
////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////
// Use sbt-release and sbt-git to add the git hash to the snapshots version numbers.
//
// see: http://blog.byjean.eu/2015/07/10/painless-release-with-sbt.html
////////////////////////////////////////////////////////////////////////////////////////////////

// git versioning settings
git.useGitDescribe := true
val VersionRegex = "v([0-9]+.[0-9]+.[0-9]+)-?(.*)?".r
git.gitTagToVersionNumber := {
  case VersionRegex(v,"") => Some(v)
  case VersionRegex(v,"SNAPSHOT") => Some(s"$v-SNAPSHOT")
  case VersionRegex(v,s) => Some(s"$v-$s-SNAPSHOT")
  case _ => None
}
// Update the base version here
git.baseVersion := "0.1.0"

// redefine the steps to set the release and next development versions to avoid writing to the version file
def setVersionOnly(selectVersion: Versions => String): ReleaseStep =  { st: State =>
  val vs = st.get(ReleaseKeys.versions).getOrElse(sys.error("No versions are set! Was this release part executed before inquireVersions?"))
  val selected = selectVersion(vs)

  st.log.info("Setting version to '%s'." format selected)
  val useGlobal =Project.extract(st).get(releaseUseGlobalVersion)
  val versionStr = (if (useGlobal) globalVersionString else versionString) format selected

  reapply(Seq(
    if (useGlobal) version in ThisBuild := selected
    else version := selected
  ), st)
}
// make sure to bump to the next version upon release, not the current one
lazy val setReleaseVersion: ReleaseStep = setVersionOnly(_._1)
releaseVersion <<= releaseVersionBump( bumper=>{
  ver => Version(ver)
    .map(_.withoutQualifier)
    .map(_.bump(bumper).string).getOrElse(versionFormatError)
})

// use a short hash
git.formattedShaVersion := git.gitHeadCommit.value map { sha => s"v$sha" }

// Release settings
releaseVersionBump := sbtrelease.Version.Bump.Next
releasePublishArtifactsAction := PgpKeys.publishSigned.value

////////////////////////////////////////////////////////////////////////////////////////////////
// For the aggregate (root) jar, override the name.  For the sub-projects,
// see the build.sbt in each project folder.
////////////////////////////////////////////////////////////////////////////////////////////////
assemblyJarName in assembly := "dagr-" + version.value + ".jar"

////////////////////////////////////////////////////////////////////////////////////////////////
// Sonatype settings
////////////////////////////////////////////////////////////////////////////////////////////////
publishMavenStyle := true
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}
publishArtifact in Test := false
pomIncludeRepository := { _ => false }

////////////////////////////////////////////////////////////////////////////////////////////////
// Coverage settings: only count coverage of dagr.sopt and dagr.core
////////////////////////////////////////////////////////////////////////////////////////////////
coverageExcludedPackages := "<empty>;dagr\\.tasks.*;dagr\\.pipelines.*;dagr\\.cmdline.*"
val htmlReportsDirectory: String = "target/test-reports"

////////////////////////////////////////////////////////////////////////////////////////////////
// scaladoc options
////////////////////////////////////////////////////////////////////////////////////////////////
val docScalacOptions = Seq("-groups", "-implicits")

////////////////////////////////////////////////////////////////////////////////////////////////
// Common settings for all projects
////////////////////////////////////////////////////////////////////////////////////////////////
lazy val commonSettings = Seq(
  organization         := "com.fulcrumgenomics",
  organizationName     := "Fulcrum Genomics LLC",
  homepage             := Some(url("http://github.com/fulcrumgenomics/dagr")),
  startYear            := Some(2015),
  scalaVersion         := "2.11.7",
  scalacOptions        += "-target:jvm-1.8",
  scalacOptions in (Compile, doc) ++= docScalacOptions,
  scalacOptions in (Test, doc) ++= docScalacOptions,
  autoAPIMappings := true,
  testOptions in Test  += Tests.Argument(TestFrameworks.ScalaTest, "-h", Option(System.getenv("TEST_HTML_REPORTS")).getOrElse(htmlReportsDirectory)),
  testOptions in Test  += Tests.Argument("-l", "LongRunningTest"), // ignores long running tests
  // uncomment for full stack traces
  //testOptions in Test  += Tests.Argument("-oD"),
  fork in Test         := true,
  test in assembly     := {},
  logLevel in assembly := Level.Info,
  resolvers            += Resolver.jcenterRepo,
  shellPrompt          := { state => "%s| %s> ".format(GitCommand.prompt.apply(state), version.value) },
  coverageExcludedPackages := "<empty>;dagr\\.tasks.*;dagr\\.pipelines.*"
) ++ Defaults.coreDefaultSettings

////////////////////////////////////////////////////////////////////////////////////////////////
// sopt project
////////////////////////////////////////////////////////////////////////////////////////////////
lazy val sopt = Project(id="dagr-sopt", base=file("sopt"))
  .settings(commonSettings: _*)
  .settings(description := "Scala command line option parser.")
  .settings(
    libraryDependencies ++= Seq(
      //---------- Test libraries -------------------//
      "org.scalatest"      %%  "scalatest"       %  "2.2.4" % "test->*" excludeAll ExclusionRule(organization="org.junit", name="junit")
    )
  )
  .enablePlugins(GitVersioning)

////////////////////////////////////////////////////////////////////////////////////////////////
// core project
////////////////////////////////////////////////////////////////////////////////////////////////
lazy val core = Project(id="dagr-core", base=file("core"))
  .settings(commonSettings: _*)
  .settings(description := "Core methods and classes to execute tasks in dagr.")
  .settings(
    libraryDependencies ++= Seq(
      "com.github.dblock"  %   "oshi-core"       %  "2.0",
      "org.scala-lang"     %   "scala-reflect"   %  scalaVersion.value,
      "org.scala-lang"     %   "scala-compiler"  %  scalaVersion.value,
      "org.reflections"    %   "reflections"     %  "0.9.10",
      "com.typesafe"       %   "config"          %  "1.3.0",
      "javax.servlet"      %   "servlet-api"     %  "2.5",
      //---------- Test libraries -------------------//
      "org.scalatest"      %%  "scalatest"       %  "2.2.4" % "test->*" excludeAll ExclusionRule(organization="org.junit", name="junit")
    )
  )
  .dependsOn(sopt)
  .enablePlugins(GitVersioning)
////////////////////////////////////////////////////////////////////////////////////////////////
// tasks project
////////////////////////////////////////////////////////////////////////////////////////////////
lazy val tasks = Project(id="dagr-tasks", base=file("tasks"))
  .settings(description := "A set of example dagr tasks.")
  .settings(commonSettings: _*)
  .dependsOn(core)
  .enablePlugins(GitVersioning)

////////////////////////////////////////////////////////////////////////////////////////////////
// pipelines project
////////////////////////////////////////////////////////////////////////////////////////////////
lazy val pipelines = Project(id="dagr-pipelines", base=file("pipelines"))
  .settings(description := "A set of example dagr pipelines.")
  .settings(commonSettings: _*)
  .dependsOn(tasks, core)
  .enablePlugins(GitVersioning)

////////////////////////////////////////////////////////////////////////////////////////////////
// root (dagr) project
////////////////////////////////////////////////////////////////////////////////////////////////
lazy val root = Project(id="dagr", base=file("."))
  .settings(commonSettings: _*)
  .settings(description := "A tool to execute tasks in directed acyclic graphs.")
  .aggregate(sopt, core, tasks, pipelines)
  .dependsOn(sopt, core, tasks, pipelines)
  .enablePlugins(GitVersioning)

////////////////////////////////////////////////////////////////////////////////////////////////
// Merge strategy for assembly
////////////////////////////////////////////////////////////////////////////////////////////////
val customMergeStrategy: String => MergeStrategy = {
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps@_*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", xs@_*) =>
    xs map {
      _.toLowerCase
    } match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps@(x :: xt) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xt =>
        MergeStrategy.discard
      case "spring.tooling" :: xt =>
        MergeStrategy.discard
      case "com.google.guava" :: xt =>
        MergeStrategy.discard
      case "services" :: xt =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.deduplicate
    }
  case "asm-license.txt" | "overview.html" =>
    MergeStrategy.discard
  case "logback.xml" =>
    MergeStrategy.first
  case _ => MergeStrategy.deduplicate
}
assemblyMergeStrategy in assembly := customMergeStrategy
