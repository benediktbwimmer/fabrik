#!/usr/bin/env node

import fs from 'node:fs'
import path from 'node:path'
import ts from 'typescript'

const [, , projectRootArg, fromFileArg, specifier] = process.argv

if (!projectRootArg || !fromFileArg || !specifier) {
  console.error('usage: resolve-module-specifier.mjs <project-root> <from-file> <specifier>')
  process.exit(2)
}

const projectRoot = path.resolve(projectRootArg)
const fromFile = path.resolve(fromFileArg)

function findNearestTsconfig(startFile, boundaryRoot) {
  let current = path.dirname(startFile)
  const boundary = path.resolve(boundaryRoot)
  while (true) {
    const candidate = path.join(current, 'tsconfig.json')
    if (fs.existsSync(candidate)) {
      return candidate
    }
    if (current === boundary) {
      return null
    }
    const parent = path.dirname(current)
    if (parent === current) {
      return null
    }
    current = parent
  }
}

function parseCompilerOptions(tsconfigPath) {
  if (!tsconfigPath) {
    return {}
  }
  const configFile = ts.readConfigFile(tsconfigPath, ts.sys.readFile)
  if (configFile.error) {
    return {}
  }
  const parsed = ts.parseJsonConfigFileContent(
    configFile.config,
    ts.sys,
    path.dirname(tsconfigPath),
    undefined,
    tsconfigPath,
  )
  return parsed.options ?? {}
}

function resolveWithTypeScript(specifierToResolve) {
  const tsconfigPath = findNearestTsconfig(fromFile, projectRoot)
  const compilerOptions = parseCompilerOptions(tsconfigPath)
  const host = ts.sys
  const resolved = ts.resolveModuleName(specifierToResolve, fromFile, compilerOptions, host)
    .resolvedModule
  if (resolved?.resolvedFileName && fs.existsSync(resolved.resolvedFileName)) {
    return path.resolve(resolved.resolvedFileName)
  }
  return null
}

function moduleResolutionCandidates(basePath) {
  const candidates = []
  if (path.extname(basePath)) {
    candidates.push(basePath)
    return candidates
  }
  for (const extension of ['.ts', '.mts', '.cts', '.js', '.mjs', '.cjs']) {
    candidates.push(`${basePath}${extension}`)
  }
  for (const indexName of ['index.ts', 'index.mts', 'index.cts', 'index.js', 'index.mjs', 'index.cjs']) {
    candidates.push(path.join(basePath, indexName))
  }
  return candidates
}

function findWorkspacePackages(rootDir) {
  const packages = []
  const stack = [rootDir]
  while (stack.length > 0) {
    const current = stack.pop()
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      if (entry.isDirectory()) {
        if (['.git', 'node_modules', 'dist', 'build', 'coverage', 'target'].includes(entry.name)) {
          continue
        }
        stack.push(path.join(current, entry.name))
        continue
      }
      if (entry.name !== 'package.json') {
        continue
      }
      const packageJsonPath = path.join(current, entry.name)
      try {
        const manifest = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'))
        if (typeof manifest.name === 'string' && manifest.name.length > 0) {
          packages.push({
            dir: current,
            name: manifest.name,
            manifest,
          })
        }
      } catch {
        // Ignore invalid manifests and continue scanning the workspace.
      }
    }
  }
  return packages
}

function resolveFromWorkspacePackage() {
  const packages = findWorkspacePackages(projectRoot)
  const matched = packages
    .filter((pkg) => specifier === pkg.name || specifier.startsWith(`${pkg.name}/`))
    .sort((a, b) => b.name.length - a.name.length)[0]
  if (!matched) {
    return null
  }

  const subpath =
    specifier === matched.name ? '' : specifier.slice(matched.name.length + 1)
  const manifest = matched.manifest ?? {}
  const sourceRoots = new Set([matched.dir])

  for (const field of ['types', 'source', 'module', 'main']) {
    if (typeof manifest[field] !== 'string' || manifest[field].length === 0) {
      continue
    }
    sourceRoots.add(path.resolve(matched.dir, path.dirname(manifest[field])))
  }

  const subpaths = new Set()
  if (subpath.length === 0) {
    for (const field of ['types', 'source', 'module', 'main']) {
      if (typeof manifest[field] === 'string' && manifest[field].length > 0) {
        subpaths.add(manifest[field])
      }
    }
    subpaths.add('index')
  } else {
    subpaths.add(subpath)
    for (const prefix of ['lib/', 'dist/', 'build/', 'src/']) {
      if (subpath.startsWith(prefix)) {
        subpaths.add(subpath.slice(prefix.length))
      }
    }
  }

  for (const root of sourceRoots) {
    for (const candidateSubpath of subpaths) {
      for (const candidate of moduleResolutionCandidates(path.resolve(root, candidateSubpath))) {
        if (fs.existsSync(candidate)) {
          return candidate
        }
      }
    }
  }
  return null
}

const resolved =
  resolveWithTypeScript(specifier) ??
  resolveFromWorkspacePackage()

process.stdout.write(JSON.stringify({ resolved }))
