// List packages for minor updates
const minorUpdatePackages = ['uuid']

module.exports = {
  target: packageName => {
    return minorUpdatePackages.includes(packageName) ? 'minor' : 'latest'
  }
}
