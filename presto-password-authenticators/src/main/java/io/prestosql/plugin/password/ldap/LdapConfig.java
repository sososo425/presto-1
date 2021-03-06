/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.password.ldap;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Strings.nullToEmpty;

public class LdapConfig
{
    private static final Logger log = Logger.get(LdapConfig.class);

    private String ldapUrl;
    private boolean allowInsecure;
    private File trustCertificate;
    private String userBindSearchPattern;
    private String groupAuthorizationSearchPattern;
    private String userBaseDistinguishedName;
    private String bindDistinguishedName;
    private String bindPassword;
    private boolean ignoreReferrals;
    private Duration ldapCacheTtl = new Duration(1, TimeUnit.HOURS);

    @NotNull
    @Pattern(regexp = "^ldaps?://.*", message = "Invalid LDAP server URL. Expected ldap:// or ldaps://")
    public String getLdapUrl()
    {
        return ldapUrl;
    }

    @Config("ldap.url")
    @ConfigDescription("URL of the LDAP server")
    public LdapConfig setLdapUrl(String url)
    {
        this.ldapUrl = url;
        return this;
    }

    public boolean isAllowInsecure()
    {
        return allowInsecure;
    }

    @Config("ldap.allow-insecure")
    @ConfigDescription("Allow insecure connection to the LDAP server")
    public LdapConfig setAllowInsecure(boolean allowInsecure)
    {
        this.allowInsecure = allowInsecure;
        return this;
    }

    @AssertTrue(message = "Connecting to the LDAP server without SSL enabled requires `ldap.allow-insecure=true`")
    public boolean isUrlConfigurationValid()
    {
        return nullToEmpty(ldapUrl).startsWith("ldaps://") || allowInsecure;
    }

    public File getTrustCertificate()
    {
        return trustCertificate;
    }

    @Config("ldap.ssl-trust-certificate")
    @ConfigDescription("Path to the PEM trust certificate for the LDAP server")
    public LdapConfig setTrustCertificate(File trustCertificate)
    {
        this.trustCertificate = trustCertificate;
        return this;
    }

    public String getUserBindSearchPattern()
    {
        return userBindSearchPattern;
    }

    @Config("ldap.user-bind-pattern")
    @ConfigDescription("Custom user bind pattern. Example: ${USER}@example.com")
    public LdapConfig setUserBindSearchPattern(String userBindSearchPattern)
    {
        this.userBindSearchPattern = userBindSearchPattern;
        return this;
    }

    public String getGroupAuthorizationSearchPattern()
    {
        return groupAuthorizationSearchPattern;
    }

    @Config("ldap.group-auth-pattern")
    @ConfigDescription("Custom group authorization check query. Example: &(objectClass=user)(memberOf=cn=group)(user=username)")
    public LdapConfig setGroupAuthorizationSearchPattern(String groupAuthorizationSearchPattern)
    {
        this.groupAuthorizationSearchPattern = groupAuthorizationSearchPattern;
        return this;
    }

    public String getUserBaseDistinguishedName()
    {
        return userBaseDistinguishedName;
    }

    @Config("ldap.user-base-dn")
    @ConfigDescription("Base distinguished name of the user. Example: dc=example,dc=com")
    public LdapConfig setUserBaseDistinguishedName(String userBaseDistinguishedName)
    {
        this.userBaseDistinguishedName = userBaseDistinguishedName;
        return this;
    }

    public String getBindDistingushedName()
    {
        return bindDistinguishedName;
    }

    @Config("ldap.bind-dn")
    @ConfigDescription("Bind distinguished name used by Presto. Example: CN=User Name,OU=CITY_OU,OU=STATE_OU,DC=domain,DC=domain_root")
    public LdapConfig setBindDistingushedName(String bindDistingushedName)
    {
        this.bindDistinguishedName = bindDistingushedName;
        return this;
    }

    public String getBindPassword()
    {
        return bindPassword;
    }

    @Config("ldap.bind-password")
    @ConfigDescription("Bind password used by Presto. Example: password1234")
    @ConfigSecuritySensitive
    public LdapConfig setBindPassword(String bindPassword)
    {
        this.bindPassword = bindPassword;
        return this;
    }

    public boolean isIgnoreReferrals()
    {
        return ignoreReferrals;
    }

    @Config("ldap.ignore-referrals")
    @ConfigDescription("Referrals allow finding entries across multiple LDAP servers. Ignore them to only search within 1 LDAP server")
    public LdapConfig setIgnoreReferrals(boolean ignoreReferrals)
    {
        this.ignoreReferrals = ignoreReferrals;
        return this;
    }

    @NotNull
    public Duration getLdapCacheTtl()
    {
        return ldapCacheTtl;
    }

    @Config("ldap.cache-ttl")
    public LdapConfig setLdapCacheTtl(Duration ldapCacheTtl)
    {
        this.ldapCacheTtl = ldapCacheTtl;
        return this;
    }
}
